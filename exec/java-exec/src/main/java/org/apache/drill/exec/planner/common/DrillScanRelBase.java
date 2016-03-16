/**
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
package org.apache.drill.exec.planner.common;

import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DrillTranslatableTable;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;

/**
 * Base class for logical scan rel implemented in Drill.
 * NOTE: we should eventually make this class independent of TableAccessRelBase and then
 * make it the base class for logical and physical scan rels.
 */
public abstract class DrillScanRelBase extends TableScan implements DrillRelNode {
  protected final DrillTable drillTable;

  public DrillScanRelBase(Convention convention, RelOptCluster cluster, RelTraitSet traits, RelOptTable table) {
    super(cluster, traits, table);
    DrillTable unwrap = table.unwrap(DrillTable.class);
    if (unwrap == null) {
      unwrap = table.unwrap(DrillTranslatableTable.class).getDrillTable();
    }
    this.drillTable = unwrap;
    assert drillTable != null;
  }

  public DrillScanRelBase(Convention convention, RelOptCluster cluster, RelTraitSet traits, RelOptTable table,
      DrillTable drillTable) {
    super(cluster, traits, table);
    this.drillTable = drillTable;
  }

  public DrillTable getDrillTable() {
    return drillTable;
  }

}
