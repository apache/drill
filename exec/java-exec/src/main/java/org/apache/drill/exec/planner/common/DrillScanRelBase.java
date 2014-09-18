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
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTraitSet;

/**
 * Base class for logical scan rel implemented in Drill.
 * NOTE: we should eventually make this class independent of TableAccessRelBase and then
 * make it the base class for logical and physical scan rels.
 */
public abstract class DrillScanRelBase extends TableAccessRelBase implements DrillRelNode {
  protected final DrillTable drillTable;

  public DrillScanRelBase(Convention convention, RelOptCluster cluster, RelTraitSet traits, RelOptTable table) {
    super(cluster, traits, table);
    this.drillTable = table.unwrap(DrillTable.class);
    assert drillTable != null;
  }

}
