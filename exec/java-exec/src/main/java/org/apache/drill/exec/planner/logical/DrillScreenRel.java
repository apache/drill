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

import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.rules.java.EnumerableConvention;
import net.hydromatic.optiq.rules.java.EnumerableRel;
import net.hydromatic.optiq.rules.java.EnumerableRelImplementor;
import net.hydromatic.optiq.rules.java.JavaRowFormat;
import net.hydromatic.optiq.rules.java.PhysType;
import net.hydromatic.optiq.rules.java.PhysTypeImpl;

import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Store;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Relational expression that converts from Drill to Enumerable. At runtime it executes a Drill query and returns the
 * results as an {@link net.hydromatic.linq4j.Enumerable}.
 */
public class DrillScreenRel extends SingleRel implements DrillRel {
  private static final Logger logger = LoggerFactory.getLogger(DrillScreenRel.class);

  private PhysType physType;

  public DrillScreenRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
    super(cluster, traitSet, input);
    assert input.getConvention() == DrillRel.CONVENTION;
    physType = PhysTypeImpl.of((JavaTypeFactory) cluster.getTypeFactory(), input.getRowType(), JavaRowFormat.ARRAY);
  }

  public PhysType getPhysType() {
    return physType;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(.1);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DrillScreenRel(getCluster(), traitSet, sole(inputs));
  }
  
  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    LogicalOperator childOp = implementor.visitChild(this, 0, getChild());
    return Store.builder().setInput(childOp).storageEngine("--SCREEN--").build();
  }

}
