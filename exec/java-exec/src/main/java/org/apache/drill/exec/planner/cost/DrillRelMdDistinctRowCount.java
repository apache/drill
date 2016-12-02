/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.planner.cost;

import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdDistinctRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.drill.exec.planner.common.DrillStatsTable;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DrillTranslatableTable;
import org.apache.drill.exec.planner.physical.ScanPrel;

public class DrillRelMdDistinctRowCount extends RelMdDistinctRowCount{
  private static final DrillRelMdDistinctRowCount INSTANCE =
      new DrillRelMdDistinctRowCount();

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.DISTINCT_ROW_COUNT.method, INSTANCE);

  @Override
  public Double getDistinctRowCount(RelNode rel, ImmutableBitSet groupKey, RexNode predicate) {
    if (rel instanceof TableScan) {
      return getDistinctRowCount((TableScan) rel, groupKey, predicate);
    }
    /*if (rel instanceof DrillScanRel) {
      return getDistinctRowCount((DrillScanRel) rel, groupKey, predicate);
    } else if (rel instanceof ScanPrel) {
      return getDistinctRowCount((ScanPrel) rel, groupKey, predicate);
    }*/ else if (rel instanceof SingleRel) {
      return RelMetadataQuery.getDistinctRowCount(((SingleRel)rel).getInput(), groupKey, predicate);
    } else if (rel instanceof RelSubset) {
      if (((RelSubset) rel).getBest() != null) {
        return RelMetadataQuery.getDistinctRowCount(((RelSubset)rel).getBest(), groupKey, predicate);
      } else {
        if (((RelSubset)rel).getOriginal() != null) {
          return RelMetadataQuery.getDistinctRowCount(((RelSubset)rel).getOriginal(), groupKey, predicate);
        } else {
          return super.getDistinctRowCount(rel, groupKey, predicate);
        }
      }
    } else {
      return super.getDistinctRowCount(rel, groupKey, predicate);
    }
  }

  /**
   * Estimates the number of rows which would be produced by a GROUP BY on the
   * set of columns indicated by groupKey.
   * column").
   */
  private Double getDistinctRowCount(TableScan scan, ImmutableBitSet groupKey, RexNode predicate) {
    DrillTable table = scan.getTable().unwrap(DrillTable.class);
    if (table == null) {
      table = scan.getTable().unwrap(DrillTranslatableTable.class).getDrillTable();
    }
    return getDistinctRowCountInternal(scan, table, groupKey, scan.getRowType(),
        RelMetadataQuery.getRowCount(scan), predicate);
  }

  /*private Double getDistinctRowCount(DrillScanRel scan, ImmutableBitSet groupKey, RexNode predicate) {
    DrillTable table = scan.getDrillTable();
    return getDistinctRowCountInternal((RelNode)scan, table, groupKey, scan.getRowType(),
        RelMetadataQuery.getRowCount(scan), predicate);
  }

  private Double getDistinctRowCount(ScanPrel scan, ImmutableBitSet groupKey, RexNode predicate) {
    DrillTable table = scan.getTable().unwrap(DrillTable.class);
    return getDistinctRowCountInternal(scan, table, groupKey, scan.getRowType(),
        RelMetadataQuery.getRowCount(scan), predicate);
  }*/


  private Double getDistinctRowCountInternal(RelNode scan, DrillTable table, ImmutableBitSet groupKey,
      RelDataType type, double rowCount, RexNode predicate) {
    double selectivity;

    if (table == null || table.getStatsTable() == null) {
      /* If there is no table or metadata (stats) table associated with scan, estimate the distinct
       * row count. Consistent with the estimation of Aggregate row count in RelMdRowCount :
       * distinctRowCount = rowCount * 10%.
       */
      //return scan.getRows() * 0.1;

      /* If there is no table or metadata (stats) table associated with scan, delegate to parent.
       */
      return super.getDistinctRowCount(scan, groupKey, predicate);
    }

    if (groupKey.length() == 0) {
      return rowCount;
    }

    /* If predicate is present, determine its selectivity to estimate filtered rows. Thereafter,
     * compute the number of distinct rows
     */
    selectivity = RelMetadataQuery.getSelectivity(scan, predicate);
    DrillStatsTable md = table.getStatsTable();
    final double rc = selectivity*rowCount;
    double s = 1.0;

    for (int i = 0; i < groupKey.length(); i++) {
      final String colName = type.getFieldNames().get(i);
      // Skip NDV, if not available
      if (!groupKey.get(i)) {
        continue;
      }
      Double d = md.getNdv(colName);
      if (d == null) {
        continue;
      }
      /* TODO: `d` may change with predicate. There are 2 cases:
       * a) predicate on group-by column - NDV will depend on the type of predicate
       * b) predicate on non-group-by column - NDV will depend on correlation with group-by column
       * */
      s *= 1 - d / rc;
    }

    return Math.min((1 - s) * rc, rowCount);
  }
}
