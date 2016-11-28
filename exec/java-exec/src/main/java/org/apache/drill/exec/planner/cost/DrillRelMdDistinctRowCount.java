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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdDistinctRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.physical.JoinPrel;

public class DrillRelMdDistinctRowCount extends RelMdDistinctRowCount{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillRelMdDistinctRowCount.class);

  private static final DrillRelMdDistinctRowCount INSTANCE =
      new DrillRelMdDistinctRowCount();

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.DISTINCT_ROW_COUNT.method, INSTANCE);

  @Override
  public Double getDistinctRowCount(RelNode rel, ImmutableBitSet groupKey, RexNode predicate) {
    if (rel instanceof DrillScanRel) {
      return getDistinctRowCount((DrillScanRel) rel, groupKey, predicate);
    } else {
      return super.getDistinctRowCount(rel, groupKey, predicate);
    }
  }

  @Override
  public Double getDistinctRowCount(Join rel, ImmutableBitSet groupKey, RexNode predicate) {
    Double count = null;
    if (rel != null) {
      if (rel instanceof JoinPrel) {
        // for Drill physical joins, don't recompute the distinct row count since it was already done
        // during logical planning; retrieve the cached value.
        count = ((JoinPrel)rel).getDistinctRowCount();
        if (count.doubleValue() < 0) {
          logger.warn("Invalid cached distinct row count for {}; recomputing..", rel.getDescription());
          count = super.getDistinctRowCount(rel, groupKey, predicate);
        }
      } else {
        count = super.getDistinctRowCount(rel, groupKey, predicate);
        if (count != null && rel instanceof DrillJoinRel) {
          // for Drill logical joins, cache the distinct row count such that it can be re-used during
          // physical planning
          ((DrillJoinRel)rel).setDistinctRowCount(count.doubleValue());
        }
      }
    }
    return count;
  }

  private Double getDistinctRowCount(DrillScanRel scan, ImmutableBitSet groupKey, RexNode predicate) {
    // Consistent with the estimation of Aggregate row count in RelMdRowCount : distinctRowCount = rowCount * 10%.
    return scan.getRows() * 0.1;
  }

}
