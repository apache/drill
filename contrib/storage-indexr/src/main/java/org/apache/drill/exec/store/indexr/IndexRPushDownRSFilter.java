/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.indexr;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.BroadcastExchangePrel;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.HashAggPrel;
import org.apache.drill.exec.planner.physical.HashJoinPrel;
import org.apache.drill.exec.planner.physical.LimitPrel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.planner.physical.ValuesPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Predicate;

import io.indexr.segment.rc.RCOperator;
import io.indexr.util.Trick;

public class IndexRPushDownRSFilter {
  private static final Logger log = LoggerFactory.getLogger(IndexRPushDownRSFilter.class);

  private static void setRSFilter(RelOptRuleCall call, FilterPrel filter, ProjectPrel project, ScanPrel scan, RexNode condition) {
    GroupScan gs = scan.getGroupScan();
    if (gs == null || !(gs instanceof IndexRGroupScan)) {
      return;
    }

    IndexRGroupScan groupScan = (IndexRGroupScan) gs;
    IndexRScanSpec scanSpec = groupScan.getScanSpec();
    if (scanSpec.getRSFilter() != null) {
      return;
    }

    LogicalExpression conditionExp = DrillOptiq.toDrill(new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner())), scan, condition);
    RSFilterGenerator generator = new RSFilterGenerator(groupScan, conditionExp);
    RCOperator rsFilter = generator.rsFilter();
    log.debug("================= rsFilter:" + rsFilter);

    IndexRScanSpec newScanSpec = new IndexRScanSpec(
        scanSpec.getTableName(),
        rsFilter);

    // We also need to update the old scan node with new scanSpec, prevent too many recaculations.
    groupScan.setScanSpec(newScanSpec);

    IndexRGroupScan newGroupScan = new IndexRGroupScan(
        groupScan.getUserName(),
        groupScan.getStoragePlugin(),
        newScanSpec,
        groupScan.getColumns(),
        groupScan.getLimitScanRows(),
        groupScan.getScanId());

    ScanPrel newScanPrel = ScanPrel.create(scan, filter.getTraitSet(), newGroupScan, scan.getRowType());
    // Depending on whether is a project in the middle, assign either scan or copy of project to childRel.
    RelNode childRel = project == null ? newScanPrel : project.copy(project.getTraitSet(), ImmutableList.of((RelNode) newScanPrel));

    call.transformTo(filter.copy(filter.getTraitSet(), ImmutableList.of(childRel)));
  }

  public static StoragePluginOptimizerRule FilterScan = new StoragePluginOptimizerRule(
      RelOptHelper.some(FilterPrel.class,
          RelOptHelper.any(ScanPrel.class)), "IndexRFilterScan") {
    @Override
    public void onMatch(RelOptRuleCall call) {
      FilterPrel filter = (FilterPrel) call.rel(0);
      ScanPrel scan = (ScanPrel) call.rel(1);
      RexNode condition = filter.getCondition();

      setRSFilter(call, filter, null, scan, condition);
    }
  };

  public static StoragePluginOptimizerRule FilterProjectScan = new StoragePluginOptimizerRule(
      RelOptHelper.some(FilterPrel.class,
          RelOptHelper.some(ProjectPrel.class,
              RelOptHelper.any(ScanPrel.class))), "IndexRFilterProjectScan") {
    @Override
    public void onMatch(RelOptRuleCall call) {
      FilterPrel filter = (FilterPrel) call.rel(0);
      ProjectPrel project = (ProjectPrel) call.rel(1);
      ScanPrel scan = (ScanPrel) call.rel(2);
      RexNode condition = RelOptUtil.pushFilterPastProject(filter.getCondition(), project);

      setRSFilter(call, filter, project, scan, condition);
    }
  };

  public static StoragePluginOptimizerRule HashJoinScan = new StoragePluginOptimizerRule(
      RelOptHelper.some(HashJoinPrel.class,
          RelOptHelper.some(ProjectPrel.class,
              RelOptHelper.any(ScanPrel.class)),
          RelOptHelper.some(BroadcastExchangePrel.class,
              RelOptHelper.some(HashAggPrel.class,
                  RelOptHelper.any(ValuesPrel.class))))

      //RelOptHelper.some(HashJoinPrel.class,
      //    RelOptHelper.some(ProjectPrel.class,
      //        RelOptHelper.any(ScanPrel.class)),
      //    RelOptHelper.some(HashAggPrel.class,
      //        RelOptHelper.any(ValuesPrel.class)))
      , "JoinFromIn") {
    @Override
    public void onMatch(RelOptRuleCall call) {
      log.debug("=========onMatch JoinFromIn");
      if (true) {
        return;
      }

      LimitPrel hashJoin = (LimitPrel) call.rel(0);
      ScanPrel scan = (ScanPrel) call.rel(1);
      List<RelNode> children = hashJoin.getInputs();
      BroadcastExchangePrel broadcastExchange = (BroadcastExchangePrel) Trick.find(
          children,
          new Predicate<RelNode>() {
            @Override
            public boolean test(RelNode node) {
              return node instanceof BroadcastExchangePrel;
            }
          });
      if (broadcastExchange == null) {
        return;
      }
      if (!(broadcastExchange.getInput() instanceof HashAggPrel)) {
        return;
      }
      HashAggPrel hashAgg = (HashAggPrel) broadcastExchange.getInput();
      if (!(hashAgg.getInput() instanceof ValuesPrel)) {
        return;
      }
      ValuesPrel values = (ValuesPrel) hashAgg.getInput();

    }
  };

  //private static List<RelNode> fetchList

}
