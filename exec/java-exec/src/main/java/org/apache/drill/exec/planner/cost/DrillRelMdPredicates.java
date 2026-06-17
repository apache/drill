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
package org.apache.drill.exec.planner.cost;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdPredicates;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.Util;
import org.apache.drill.exec.store.plan.rel.PluginFilterRel;

public class DrillRelMdPredicates extends RelMdPredicates {

  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider
    .reflectiveSource(new DrillRelMdPredicates(), BuiltInMetadata.Predicates.Handler.class);

  /**
   * A plugin filter is pushed down to the storage/format plugin, but for some
   * plugins (e.g. Delta Lake) it is only used to prune files conservatively and
   * does not guarantee that the rows it returns satisfy the filter condition --
   * a residual Drill filter is kept to remove non-matching rows. If we advertise
   * the condition as a pulled-up predicate, Calcite 1.42 treats that residual
   * filter as redundant and removes it, returning rows that should have been
   * filtered out. Only propagate the input's predicates for plugin filters.
   */
  public RelOptPredicateList getPredicates(PluginFilterRel filter, RelMetadataQuery mq) {
    return mq.getPulledUpPredicates(filter.getInput());
  }

  /**
   * Add the Filter condition to the pulledPredicates list from the input.
   */
  public RelOptPredicateList getPredicates(Filter filter, RelMetadataQuery mq) {
    final RelNode input = filter.getInput();
    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    final RelOptPredicateList inputInfo = mq.getPulledUpPredicates(input);

    return Util.first(inputInfo, RelOptPredicateList.EMPTY)
      .union(rexBuilder,
        RelOptPredicateList.of(rexBuilder,
          RexUtil.retainDeterministic(
            RelOptUtil.conjunctions(RexUtil.expandSearch(rexBuilder, null, filter.getCondition())))));
  }
}
