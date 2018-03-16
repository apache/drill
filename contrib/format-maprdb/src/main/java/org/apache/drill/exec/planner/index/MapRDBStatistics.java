/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.index;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.SqlKind;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.ojai.store.QueryCondition;

import java.util.HashMap;
import java.util.Map;

public class MapRDBStatistics implements Statistics {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MapRDBStatistics.class);
  static final String nullConditionAsString = "<NULL>";
  private double rowKeyJoinBackIOFactor = 1.0;
  private boolean statsAvailable = false;
  private StatisticsPayload fullTableScanPayload = null;
  /*
   * The computed statistics are cached in <statsCache> so that any subsequent calls are returned
   * from the cache. The <statsCache> is a map of <RexNode, map<Index, Stats Payload>>. The <RexNode>
   * does not have a comparator so it is converted to a String for serving as a Map key. This may result
   * in logically equivalent conditions considered differently e.g. sal<10 OR sal>100, sal>100 OR sal<10
   * the second map maintains statistics per index as not all statistics are independent of the index
   * e.g. average row size.
   */
  private Map<String, Map<String, StatisticsPayload>> statsCache;
  /*
   * The filter independent computed statistics are cached in <fIStatsCache> so that any subsequent
   * calls are returned from the cache. The <fIStatsCache> is a map of <Index, Stats Payload>. This
   * cache maintains statistics per index as not all statistics are independent of the index
   * e.g. average row size.
   */
  private Map<String, StatisticsPayload> fIStatsCache;
  /*
  /*
   * The mapping between <QueryCondition> and <RexNode> is kept in <conditionRexNodeMap>. This mapping
   * is useful to obtain rowCount for condition specified as <QueryCondition> required during physical
   * planning. Again, both the <QueryCondition> and <RexNode> are converted to Strings for the lack
   * of a comparator.
   */
  private Map<String, String> conditionRexNodeMap;

  public MapRDBStatistics() {
    statsCache = new HashMap<>();
    fIStatsCache = new HashMap<>();
    conditionRexNodeMap = new HashMap<>();
  }

  public double getRowKeyJoinBackIOFactor() {
    return rowKeyJoinBackIOFactor;
  }

  @Override
  public boolean isStatsAvailable() {
    return statsAvailable;
  }

  @Override
  public String buildUniqueIndexIdentifier(IndexDescriptor idx) {
    if (idx == null) {
      return null;
    } else {
      return idx.getTableName() + "_" + idx.getIndexName();
    }
  }

  public String buildUniqueIndexIdentifier(String tableName, String idxName) {
    if (tableName == null || idxName == null) {
      return null;
    } else {
      return tableName + "_" + idxName;
    }
  }

  @Override
  /** Returns the number of rows satisfying the given FILTER condition
   *  @param condition - FILTER specified as a {@link RexNode}
   *  @param tabIdxName - The table/index identifier
   *  @return approximate rows satisfying the filter
   */
  public double getRowCount(RexNode condition, String tabIdxName, RelNode scanRel) {
    String conditionAsStr = nullConditionAsString;
    Map<String, StatisticsPayload> payloadMap;
    if ((scanRel instanceof DrillScanRel && ((DrillScanRel)scanRel).getGroupScan() instanceof DbGroupScan)
        || (scanRel instanceof ScanPrel && ((ScanPrel)scanRel).getGroupScan() instanceof DbGroupScan)) {
      if (condition == null && fullTableScanPayload != null) {
        return fullTableScanPayload.getRowCount();
      } else if (condition != null) {
        conditionAsStr = convertRexToString(condition, scanRel.getRowType());
        payloadMap = statsCache.get(conditionAsStr);
        if (payloadMap != null) {
          if (payloadMap.get(tabIdxName) != null) {
            return payloadMap.get(tabIdxName).getRowCount();
          } else {
            // We might not have computed rowcount for the given condition from the tab/index in question.
            // For rowcount it does not matter which index was used to get the rowcount for the given condition.
            // Hence, just use the first one!
            for (String payloadKey : payloadMap.keySet()) {
              if (payloadKey != null && payloadMap.get(payloadKey) != null) {
                return payloadMap.get(payloadKey).getRowCount();
              }
            }
            StatisticsPayload anyPayload = payloadMap.entrySet().iterator().next().getValue();
            return anyPayload.getRowCount();
          }
        }
      }
    }
    if (statsAvailable) {
      logger.debug("Statistics: Filter row count is UNKNOWN for filter: {}", conditionAsStr);
    }
    return ROWCOUNT_UNKNOWN;
  }

  /** Returns the number of rows satisfying the given FILTER condition
   *  @param condition - FILTER specified as a {@link QueryCondition}
   *  @param tabIdxName - The table/index identifier
   *  @return approximate rows satisfying the filter
   */
  public double getRowCount(QueryCondition condition, String tabIdxName) {
    String conditionAsStr = nullConditionAsString;
    Map<String, StatisticsPayload> payloadMap;
    if (condition != null
        && conditionRexNodeMap.get(condition.toString()) != null) {
      String rexConditionAsString = conditionRexNodeMap.get(condition.toString());
      payloadMap = statsCache.get(rexConditionAsString);
      if (payloadMap != null) {
        if (payloadMap.get(tabIdxName) != null) {
          return payloadMap.get(tabIdxName).getRowCount();
        } else {
          // We might not have computed rowcount for the given condition from the tab/index in question.
          // For rowcount it does not matter which index was used to get the rowcount for the given condition.
          // if tabIdxName is null, most likely we have found one from payloadMap and won't come to here.
          // If we come to here, we are looking for payload for an index, so let us use any index's payload first!
          for (String payloadKey : payloadMap.keySet()) {
            if (payloadKey != null && payloadMap.get(payloadKey) != null) {
              return payloadMap.get(payloadKey).getRowCount();
            }
          }
          StatisticsPayload anyPayload = payloadMap.entrySet().iterator().next().getValue();
          return anyPayload.getRowCount();
        }
      }
    } else if (condition == null
        && fullTableScanPayload != null) {
      return fullTableScanPayload.getRowCount();
    }
    if (condition != null) {
      conditionAsStr = condition.toString();
    }
    if (statsAvailable) {
      logger.debug("Statistics: Filter row count is UNKNOWN for filter: {}", conditionAsStr);
    }
    return ROWCOUNT_UNKNOWN;
  }

  /** Returns the number of leading rows satisfying the given FILTER condition
   *  @param condition - FILTER specified as a {@link RexNode}
   *  @param tabIdxName - The table/index identifier
   *  @param scanRel - The current scanRel
   *  @return approximate rows satisfying the leading filter
   */
  @Override
  public double getLeadingRowCount(RexNode condition, String tabIdxName, DrillScanRelBase scanRel) {
    String conditionAsStr = nullConditionAsString;
    Map<String, StatisticsPayload> payloadMap;
    if ((scanRel instanceof DrillScanRel && ((DrillScanRel)scanRel).getGroupScan() instanceof DbGroupScan)
        || (scanRel instanceof ScanPrel && ((ScanPrel)scanRel).getGroupScan() instanceof DbGroupScan)) {
      if (condition == null && fullTableScanPayload != null) {
        return fullTableScanPayload.getLeadingRowCount();
      } else if (condition != null) {
        conditionAsStr = convertRexToString(condition, scanRel.getRowType());
        payloadMap = statsCache.get(conditionAsStr);
        if (payloadMap != null) {
          if (payloadMap.get(tabIdxName) != null) {
            return payloadMap.get(tabIdxName).getLeadingRowCount();
          }
          // Unlike rowcount, leading rowcount is dependent on the index. So, if tab/idx is
          // not found, we are out of luck!
        }
      }
    }
    if (statsAvailable) {
      logger.debug("Statistics: Leading filter row count is UNKNOWN for filter: {}", conditionAsStr);
    }
    return ROWCOUNT_UNKNOWN;
  }

  /** Returns the number of leading rows satisfying the given FILTER condition
   *  @param condition - FILTER specified as a {@link QueryCondition}
   *  @param tabIdxName - The table/index identifier
   *  @return approximate rows satisfying the leading filter
   */
  public double getLeadingRowCount(QueryCondition condition, String tabIdxName) {
    String conditionAsStr = nullConditionAsString;
    Map<String, StatisticsPayload> payloadMap;
    if (condition != null
        && conditionRexNodeMap.get(condition.toString()) != null) {
      String rexConditionAsString = conditionRexNodeMap.get(condition.toString());
      payloadMap = statsCache.get(rexConditionAsString);
      if (payloadMap != null) {
        if (payloadMap.get(tabIdxName) != null) {
          return payloadMap.get(tabIdxName).getLeadingRowCount();
        }
        // Unlike rowcount, leading rowcount is dependent on the index. So, if tab/idx is
        // not found, we are out of luck!
      }
    } else if (condition == null
        && fullTableScanPayload != null) {
      return fullTableScanPayload.getLeadingRowCount();
    }
    if (condition != null) {
      conditionAsStr = condition.toString();
    }
    if (statsAvailable) {
      logger.debug("Statistics: Leading filter row count is UNKNOWN for filter: {}", conditionAsStr);
    }
    return ROWCOUNT_UNKNOWN;
  }

  @Override
  public double getAvgRowSize(String tabIdxName, boolean isTableScan) {
    StatisticsPayload payloadMap;
    if (isTableScan && fullTableScanPayload != null) {
      return fullTableScanPayload.getAvgRowSize();
    } else if (!isTableScan) {
      payloadMap = fIStatsCache.get(tabIdxName);
      if (payloadMap != null) {
        return payloadMap.getAvgRowSize();
      }
    }
    if (statsAvailable) {
      logger.debug("Statistics: Average row size is UNKNOWN for table: {}", tabIdxName);
    }
    return AVG_ROWSIZE_UNKNOWN;
  }

  public boolean initialize(RexNode condition, DrillScanRelBase scanRel, IndexCallContext context) {
    //XXX to implement for complete secondary index framework
    return false;
  }

  /*
 * Convert the given RexNode to a String representation while also replacing the RexInputRef references
 * to actual column names. Since, we compare String representations of RexNodes, two equivalent RexNode
 * expressions may differ in the RexInputRef positions but otherwise the same.
 * e.g. $1 = 'CA' projection (State, Country) , $2 = 'CA' projection (Country, State)
 */
  private String convertRexToString(RexNode condition, RelDataType rowType) {
    StringBuilder sb = new StringBuilder();
    if (condition == null) {
      return null;
    }
    if (condition.getKind() == SqlKind.AND) {
      boolean first = true;
      for(RexNode pred : RelOptUtil.conjunctions(condition)) {
        if (first) {
          sb.append(convertRexToString(pred, rowType));
          first = false;
        } else {
          sb.append(" " + SqlKind.AND.toString() + " ");
          sb.append(convertRexToString(pred, rowType));
        }
      }
      return sb.toString();
    } else if (condition.getKind() == SqlKind.OR) {
      boolean first = true;
      for(RexNode pred : RelOptUtil.disjunctions(condition)) {
        if (first) {
          sb.append(convertRexToString(pred, rowType));
          first = false;
        } else {
          sb.append(" " + SqlKind.OR.toString() + " ");
          sb.append(convertRexToString(pred, rowType));
        }
      }
      return sb.toString();
    } else {
      HashMap<String, String> inputRefMapping = new HashMap<>();
      /* Based on the rel projection the input reference for the same column may change
       * during planning. We want the cache to be agnostic to it. Hence, the entry stored
       * in the cache has the input reference ($i) replaced with the column name
       */
      getInputRefMapping(condition, rowType, inputRefMapping);
      if (inputRefMapping.keySet().size() > 0) {
        //Found input ref - replace it
        String replCondition = condition.toString();
        for (String inputRef : inputRefMapping.keySet()) {
          replCondition = replCondition.replace(inputRef, inputRefMapping.get(inputRef));
        }
        return replCondition;
      } else {
        return condition.toString();
      }
    }
  }

  /*
 * Generate the input reference to column mapping for reference replacement. Please
 * look at the usage in convertRexToString() to understand why this mapping is required.
 */
  private void getInputRefMapping(RexNode condition, RelDataType rowType,
                                  HashMap<String, String> mapping) {
    if (condition instanceof RexCall) {
      for (RexNode op : ((RexCall) condition).getOperands()) {
        getInputRefMapping(op, rowType, mapping);
      }
    } else if (condition instanceof RexInputRef) {
      mapping.put(condition.toString(),
          rowType.getFieldNames().get(condition.hashCode()));
    }
  }
}
