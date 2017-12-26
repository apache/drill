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
package org.apache.drill.exec.planner.cost;

import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdDistinctRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.drill.exec.planner.logical.DrillScanRel;

public class DrillRelMdDistinctRowCount extends RelMdDistinctRowCount {
  private static final DrillRelMdDistinctRowCount INSTANCE =
      new DrillRelMdDistinctRowCount();

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.DISTINCT_ROW_COUNT.method, INSTANCE);

  /**
   * We need to override this method since Calcite and Drill calculate
   * joined row count in different ways. It helps avoid a case when
   * at the first time was used Drill join row count but at the second time
   * Calcite row count was used. It may happen when
   * {@link RelMdDistinctRowCount#getDistinctRowCount(Join, RelMetadataQuery,
   * ImmutableBitSet, RexNode)} method is used and after that used
   * another getDistinctRowCount method for parent rel, which just uses
   * row count of input rel node (our join rel).
   * It causes cost increase of best rel node when
   * {@link RelSubset#propagateCostImprovements} is called.
   *
   * This is a part of the fix for CALCITE-2018.
   */
  @Override
  public Double getDistinctRowCount(Join rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey, RexNode predicate) {
    return getDistinctRowCount((RelNode) rel, mq, groupKey, predicate);
  }

  public Double getDistinctRowCount(DrillScanRel scan, RelMetadataQuery mq,
      ImmutableBitSet groupKey, RexNode predicate) {
    // Consistent with the estimation of Aggregate row count in RelMdRowCount : distinctRowCount = rowCount * 10%.
    return scan.getRows() * 0.1;
  }
}
