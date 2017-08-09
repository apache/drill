/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to you under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.drill.exec.planner.physical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

/**
 * Represents a Prel implementation with information about row count and max row count.
 */
public abstract class AbstractPrel extends AbstractRelNode implements Prel {

  private final double rowCount;
  private final double maxRowCount;

  public AbstractPrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
    super(cluster, traitSet);
    RelMetadataQuery relMetadataQuery = RelMetadataQuery.instance();
    this.rowCount = relMetadataQuery.getRowCount(input);
    this.maxRowCount = relMetadataQuery.getMaxRowCount(input);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return rowCount;
  }

  public double getMaxRowCount() {
    return maxRowCount;
  }
}
