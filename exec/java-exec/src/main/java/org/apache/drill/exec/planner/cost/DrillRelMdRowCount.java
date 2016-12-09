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

import java.io.IOException;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.drill.exec.planner.common.DrillFilterRelBase;
import org.apache.drill.exec.planner.common.DrillRelOptUtil;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DrillTranslatableTable;
import org.apache.drill.exec.store.parquet.ParquetGroupScan;

public class DrillRelMdRowCount extends RelMdRowCount{
  private static final DrillRelMdRowCount INSTANCE = new DrillRelMdRowCount();

  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(BuiltInMethod.ROW_COUNT.method, INSTANCE);

  @Override
  public Double getRowCount(RelNode rel) {
    if (rel instanceof DrillScanRelBase) {
      return getRowCount((DrillScanRelBase) rel);
    } else if (rel instanceof DrillFilterRelBase) {
      return getRowCount((DrillFilterRelBase) rel);
    } else {
      return super.getRowCount(rel);
    }
  }

  private Double getRowCount(DrillFilterRelBase rel) {
    if (DrillRelOptUtil.guessRows(rel)) {
      return super.getRowCount(rel);
    }
    // Need capped selectivity estimates. See the Filter getRows() method
    return rel.getRows();
  }

  private Double getRowCount(DrillScanRelBase rel) {
    DrillTable table;
    if (DrillRelOptUtil.guessRows(rel)) {
      return super.getRowCount(rel);
    }
    table = rel.getTable().unwrap(DrillTable.class);
    if (table == null) {
      table = rel.getTable().unwrap(DrillTranslatableTable.class).getDrillTable();
    }
    // Return rowcount from statistics, if available. Otherwise, delegate to parent.
    try {
      if (table != null
          && table.getStatsTable() != null
          /* For ParquetGroupScan rely on accurate count from the scan instead of
           * statistics since partition pruning/filter pushdown might have occurred.
           * The other way would be to iterate over the rowgroups present in the
           * ParquetGroupScan to obtain the rowcount.
           */
          && !(table.getGroupScan() instanceof ParquetGroupScan)) {
        return table.getStatsTable().getRowCount();
      }
    } catch (IOException ex) {
      return super.getRowCount(rel);
    }
    return super.getRowCount(rel);
  }
}
