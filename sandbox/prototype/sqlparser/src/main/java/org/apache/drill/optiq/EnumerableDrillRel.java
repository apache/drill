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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import net.hydromatic.linq4j.expressions.BlockBuilder;
import net.hydromatic.linq4j.expressions.BlockStatement;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.linq4j.function.Functions;
import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.rules.java.EnumerableConvention;
import net.hydromatic.optiq.rules.java.EnumerableRel;
import net.hydromatic.optiq.rules.java.EnumerableRelImplementor;
import net.hydromatic.optiq.rules.java.JavaRowFormat;
import net.hydromatic.optiq.rules.java.PhysType;
import net.hydromatic.optiq.rules.java.PhysTypeImpl;

import org.apache.drill.common.util.Hook;
import org.apache.drill.exec.client.DrillClient;
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
public class EnumerableDrillRel extends SingleRel implements EnumerableRel {
  private static final Logger logger = LoggerFactory.getLogger(EnumerableDrillRel.class);

  private static final Function1<String, Expression> TO_LITERAL = new Function1<String, Expression>() {
    @Override
    public Expression apply(String a0) {
      return Expressions.constant(a0);
    }
  };

  private static final Method OF_METHOD;

  private PhysType physType;
  private DrillClient client;
  
  static {
    try {
      OF_METHOD = EnumerableDrillFullEngine.class.getMethod("of", String.class, List.class, Class.class,
          DataContext.class);
      // EnumerableDrillFullEngine.class.getMethod("of", String.class, List.class, Class.class);
    } catch (NoSuchMethodException e) {
      logger.error("Exception during query", e);
      throw new RuntimeException(e);
    }
  }

  public EnumerableDrillRel(DrillClient client, RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
    super(cluster, traitSet, input);
    assert getConvention() instanceof EnumerableConvention;
    assert input.getConvention() == DrillRel.CONVENTION;
    this.client = client;
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
    return new EnumerableDrillRel(client, getCluster(), traitSet, sole(inputs));
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    logger.debug("implementing enumerable");

    final DrillImplementor drillImplementor = new DrillImplementor();
    DrillRel input = (DrillRel) getChild();

    drillImplementor.go(input);
    String plan = drillImplementor.getJsonString();
    Hook.LOGICAL_PLAN.run(plan);

    // not quite sure where this list was supposed to be set earlier, leaving it null got me back the full result set

    final List<String> fieldNameList = rowType.getFieldNames();
    // final List<String> fieldNameList = null;
    BlockStatement expr = new BlockBuilder().append(
        Expressions.call(
            OF_METHOD,
            Expressions.constant(plan), //
            Expressions.call(Arrays.class, "asList", //
                Expressions.newArrayInit(String.class, Functions.apply(fieldNameList, TO_LITERAL)) //
                ),
            Expressions.constant(Object.class), //
            Expressions.variable(DataContext.class, "root"))).toBlock();
    final PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(),
        pref.prefer(JavaRowFormat.ARRAY));
    return new Result(expr, physType, JavaRowFormat.ARRAY);
  }

}
