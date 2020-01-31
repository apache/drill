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
package org.apache.drill.exec.store.base;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.base.filter.DisjunctionFilterSpec;
import org.apache.drill.exec.store.base.filter.FilterPushDownListener;
import org.apache.drill.exec.store.base.filter.FilterPushDownStrategy;
import org.apache.drill.exec.store.base.filter.FilterSpec;
import org.apache.drill.exec.store.base.filter.RelOp;

public class DummyFilterPushDownListener implements FilterPushDownListener {

  private final DummyStoragePluginConfig config;

  public DummyFilterPushDownListener(DummyStoragePluginConfig config) {
    this.config = config;
  }

  public static Set<StoragePluginOptimizerRule> rulesFor(
      OptimizerRulesContext optimizerRulesContext, DummyStoragePluginConfig config) {
    return FilterPushDownStrategy.rulesFor(
        new DummyFilterPushDownListener(config));
  }

  /**
   * Prefix that appears in logging of the Calcite rules used by
   * filter push-down.
   */
  @Override
  public String prefix() { return "Dummy"; }

  @Override
  public boolean isTargetScan(GroupScan groupScan) {
    return groupScan instanceof DummyGroupScan;
  }

  /**
   * Report if we need to apply the rule. Here it means not to
   * apply the rules to this group scan if we've already done so.
   * The rules may still be applied multiple times to different
   * group scan "versions" as Calcite explores different plan
   * variations.
   */
  @Override
  public boolean needsApplication(GroupScan groupScan) {
    DummyGroupScan dummyScan = (DummyGroupScan) groupScan;
    return !dummyScan.hasFilters();
  }

  @Override
  public RelOp accept(GroupScan groupScan, RelOp relOp) {

    // Determine if filter applies to this scan
    DummyGroupScan dummyScan = (DummyGroupScan) groupScan;
    return dummyScan.acceptFilter(relOp);
  }

  /**
   * Accept the analyzed constant expressions (or not.) Here, we let
   * the group scan decide based on some ad-hoc rules established for
   * testing. We return the the RexNodes if the plugin is configured
   * to keep the nodes in the query, we discard them (claim that the
   * scan will implement them) otherwise. This allows testing both
   * paths. A real implementation would do one or the other.
   */
  @Override
  public Pair<GroupScan, List<RexNode>> transform(GroupScan groupScan,
      List<Pair<RexNode, RelOp>> andTerms, Pair<RexNode, DisjunctionFilterSpec> orTerm) {
    List<RelOp> andExprs;
    if (andTerms == null || andTerms.isEmpty()) {
      andExprs = null;
    } else {
      andExprs = andTerms.stream()
          .map(t -> t.right)
          .collect(Collectors.toList());
    }
    DisjunctionFilterSpec orExprs;
    if (orTerm == null) {
      orExprs = null;
    } else {
      orExprs = orTerm.right;
    }
    FilterSpec filters = FilterSpec.build(andExprs, orExprs);
    DummyGroupScan dummyScan = (DummyGroupScan) groupScan;
    GroupScan newScan = new DummyGroupScan(dummyScan, filters);

    List<RexNode> exprs;
    if (config.keepFilters()) {
      exprs = new ArrayList<>();
      if (andTerms != null) {
        exprs.addAll(andTerms.stream()
            .map(t -> t.left)
            .collect(Collectors.toList()));
      }
      if (orTerm != null) {
        exprs.add(orTerm.left);
      }
    } else {
      exprs = null;
    }
    return Pair.of(newScan, exprs);
  }
}
