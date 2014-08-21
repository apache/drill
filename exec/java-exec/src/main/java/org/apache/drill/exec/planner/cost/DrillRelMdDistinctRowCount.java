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

import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMdDistinctRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.drill.exec.planner.common.DrillStatsTable;
import org.apache.drill.exec.planner.logical.DrillScanRel;

import java.util.List;

public class DrillRelMdDistinctRowCount extends RelMdDistinctRowCount{
  private static final DrillRelMdDistinctRowCount INSTANCE =
      new DrillRelMdDistinctRowCount();

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.DISTINCT_ROW_COUNT.method, INSTANCE);

  @Override
  public Double getDistinctRowCount(RelNode rel, ImmutableBitSet groupKey, RexNode predicate) {
    if (rel instanceof DrillScanRel) {
      return getDistinctRowCount((DrillScanRel) rel, groupKey);
    } else {
      return super.getDistinctRowCount(rel, groupKey, predicate);
    }
  }

  /**
   * Estimates the number of rows which would be produced by a GROUP BY on the
   * set of columns indicated by groupKey.
   * column").
   */
  private Double getDistinctRowCount(DrillScanRel scan, ImmutableBitSet groupKey) {
    if (scan.getDrillTable() == null || scan.getDrillTable().getStatsTable() == null) {
      // If there is no table or metadata (stats) table associated with scan, estimate the distinct row count.
      // Consistent with the estimation of Aggregate row count in RelMdRowCount : distinctRowCount = rowCount * 10%.
      return scan.getRows() * 0.1;
    }

    // TODO: may be we should get the column origin of each group by key before we look up it in metadata table?
    List<RelColumnOrigin> cols = Lists.newArrayList();

    if (groupKey.length() == 0) {
      return new Double(0);
    }

    DrillStatsTable md = scan.getDrillTable().getStatsTable();

    final double rc = RelMetadataQuery.getRowCount(scan);
    double s = 1.0;
    for (int i = 0; i < groupKey.length(); i++) {
      final String colName = scan.getRowType().getFieldNames().get(i);
      if (!groupKey.get(i) && colName.equals("*")) {
        continue;
      }

      Double d = md.getNdv(colName);
      if (d == null) {
        continue;
      }

      s *= 1 - d / rc;
    }
    return new Double((1 - s) * rc);
  }

}
