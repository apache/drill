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
package org.apache.drill.optiq;

import net.hydromatic.linq4j.expressions.BlockBuilder;
import net.hydromatic.linq4j.expressions.BlockExpression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.rules.java.*;

import org.apache.drill.common.util.Hook;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.List;


/**
 * Relational expression that converts from Drill to Enumerable. At runtime
 * it executes a Drill query and returns the results as an
 * {@link net.hydromatic.linq4j.Enumerable}.
 */
public class EnumerableDrillRel extends SingleRel implements EnumerableRel {
  private static final Logger LOG =
      LoggerFactory.getLogger(EnumerableDrillRel.class);

  private PhysType physType;

  public EnumerableDrillRel(RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input)
  {
    super(cluster, traitSet, input);
    assert getConvention() instanceof EnumerableConvention;
    physType = PhysTypeImpl.of((JavaTypeFactory) cluster.getTypeFactory(),
        input.getRowType(),
        (EnumerableConvention) getConvention());
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
    return new EnumerableDrillRel(getCluster(), traitSet, sole(inputs));
  }

  public BlockExpression implement(EnumerableRelImplementor implementor) {
    LOG.debug("implementing enumerable");

    final DrillImplementor drillImplementor = new DrillImplementor();
    DrillRel input = (DrillRel) getChild();

    drillImplementor.go(input);
    final Method method;
    try {
      method =
          EnumerableDrill.class.getMethod("of", String.class, Class.class);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
    String plan = drillImplementor.getJsonString();
    Hook.LOGICAL_PLAN.run(plan);
    return new BlockBuilder()
        .append(
            Expressions.call(
                method,
                Expressions.constant(plan),
                Expressions.constant(Class.class)))
        .toBlock();
  }
}

// End EnumerableDrillRel.java
