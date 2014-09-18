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
package org.apache.drill.exec.planner.logical;

import java.util.List;

import net.hydromatic.optiq.prepare.Prepare.CatalogReader;

import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.exec.planner.common.DrillStoreRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTraitSet;

public class DrillStoreRel extends DrillStoreRelBase implements DrillRel{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillStoreRel.class);

  protected DrillStoreRel(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, CatalogReader catalogReader,
      RelNode child, Operation operation, List<String> updateColumnList, boolean flattened) {
    super(cluster, traits, table, catalogReader, child, operation, updateColumnList, flattened);

  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    return null;
  }

}
