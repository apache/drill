/*
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
package org.apache.drill.exec.store.jdbc;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;

import org.apache.drill.shaded.guava.com.google.common.cache.CacheBuilder;
import org.apache.drill.shaded.guava.com.google.common.cache.CacheLoader;
import org.apache.drill.shaded.guava.com.google.common.cache.LoadingCache;
import org.apache.drill.exec.planner.logical.DrillRelFactories;

abstract class DrillJdbcRuleBase extends ConverterRule {

  protected final LoadingCache<RexNode, Boolean> checkedExpressions = CacheBuilder.newBuilder()
      .maximumSize(1000)
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .build(
          new CacheLoader<RexNode, Boolean>() {
            public Boolean load(RexNode expr) {
              return JdbcExpressionCheck.isOnlyStandardExpressions(expr);
            }
          });

  protected final JdbcConvention out;

  private DrillJdbcRuleBase(Class<? extends RelNode> clazz, RelTrait in, JdbcConvention out, String description) {
    super(clazz, (Predicate<RelNode>) input -> true, in, out, DrillRelFactories.LOGICAL_BUILDER, description);
    this.out = out;
  }

  static class DrillJdbcProjectRule extends DrillJdbcRuleBase {

    public DrillJdbcProjectRule(JdbcConvention out) {
      super(LogicalProject.class, Convention.NONE, out, "JdbcProjectRule");
    }

    public RelNode convert(RelNode rel) {
      LogicalProject project = (LogicalProject) rel;
      return new JdbcRules.JdbcProject(rel.getCluster(), rel.getTraitSet().replace(this.out), convert(
          project.getInput(), project.getInput().getTraitSet().replace(this.out).simplify()), project.getProjects(),
          project.getRowType());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      try {

        final LogicalProject project = call.rel(0);
        for (RexNode node : project.getChildExps()) {
          if (!checkedExpressions.get(node)) {
            return false;
          }
        }
        return true;

      } catch (ExecutionException e) {
        throw new IllegalStateException("Failure while trying to evaluate pushdown.", e);
      }
    }
  }

  static class DrillJdbcFilterRule extends DrillJdbcRuleBase {

    public DrillJdbcFilterRule(JdbcConvention out) {
      super(LogicalFilter.class, Convention.NONE, out, "DrillJdbcFilterRule");
    }

    public RelNode convert(RelNode rel) {
      LogicalFilter filter = (LogicalFilter) rel;

      return new JdbcRules.JdbcFilter(rel.getCluster(), rel.getTraitSet().replace(this.out), convert(filter.getInput(),
          filter.getInput().getTraitSet().replace(this.out).simplify()), filter.getCondition());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      try {

        final LogicalFilter filter = call.rel(0);
        for (RexNode node : filter.getChildExps()) {
          if (!checkedExpressions.get(node)) {
            return false;
          }
        }
        return true;

      } catch (ExecutionException e) {
        throw new IllegalStateException("Failure while trying to evaluate pushdown.", e);
      }
    }
  }

}
