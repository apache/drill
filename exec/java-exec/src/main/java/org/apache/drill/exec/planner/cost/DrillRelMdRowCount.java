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
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.DrillTable;

public class DrillRelMdRowCount extends RelMdRowCount{
  private static final DrillRelMdRowCount INSTANCE = new DrillRelMdRowCount();

  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(BuiltInMethod.ROW_COUNT.method, INSTANCE);

  @Override
  public Double getRowCount(Aggregate rel) {
    ImmutableBitSet groupKey = ImmutableBitSet.range(rel.getGroupCount());

    if (groupKey.isEmpty()) {
      return 1.0;
    } else {
      return super.getRowCount(rel);
    }
  }

  @Override
  public Double getRowCount(RelNode rel) {
    if (rel instanceof DrillScanRel) {
      return getRowCount((DrillScanRel)rel);
    } else if (rel instanceof TableScan) {
      return getRowCount((TableScan) rel);
    } else if (rel instanceof Filter) {
      return getRowCount(rel);
    }
    return super.getRowCount(rel);
  }

  @Override
  public Double getRowCount(Filter rel) {
    // Need capped selectivity estimates. See getRows()
    return Double.valueOf(rel.getRows());
  }

  private Double getRowCount(DrillScanRel scanRel) {
    final DrillTable table = scanRel.getDrillTable();
    // Return rowcount from statistics, if available. Otherwise, delegate to parent.
    if (table != null
        && table.getStatsTable() != null) {
      return table.getStatsTable().getRowCount();
    }
    return super.getRowCount(scanRel);
  }

  private Double getRowCount(TableScan scanRel) {
    final DrillTable table = scanRel.getTable().unwrap(DrillTable.class);
    // Return rowcount from statistics, if available. Otherwise, delegate to parent.
    if (table != null
       && table.getStatsTable() != null) {
      return table.getStatsTable().getRowCount();
    }
    return super.getRowCount(scanRel);
  }
}
