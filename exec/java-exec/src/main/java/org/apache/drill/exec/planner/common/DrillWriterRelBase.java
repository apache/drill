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

import org.apache.drill.exec.planner.logical.CreateTableEntry;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;

/** Base class for logical and physical Writer implemented in Drill. */
public abstract class DrillWriterRelBase extends SingleRel implements DrillRelNode {

  private final CreateTableEntry createTableEntry;

  public DrillWriterRelBase(Convention convention, RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      CreateTableEntry createTableEntry) {
    super(cluster, traitSet, input);
    assert input.getConvention() == convention;
    this.createTableEntry = createTableEntry;
  }

  public CreateTableEntry getCreateTableEntry() {
    return createTableEntry;
  }
}
