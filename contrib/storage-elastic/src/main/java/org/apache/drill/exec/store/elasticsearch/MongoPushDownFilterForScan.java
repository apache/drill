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
package org.apache.drill.exec.store.elasticsearch;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

/**
 * This is an optimizer
 */
public class MongoPushDownFilterForScan extends StoragePluginOptimizerRule {
  private static final Logger logger = LoggerFactory.getLogger(MongoPushDownFilterForScan.class);

  public static final StoragePluginOptimizerRule INSTANCE = new MongoPushDownFilterForScan();

  private MongoPushDownFilterForScan() {
    super(RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)), "MongoPushDownFilterForScan");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ScanPrel scan = (ScanPrel) call.rel(1);
    final FilterPrel filter = (FilterPrel) call.rel(0);
    final RexNode condition = filter.getCondition();

    ElasticSearchGroupScan groupScan = (ElasticSearchGroupScan) scan.getGroupScan();
    // Has it been pushed down?
    if (groupScan.isFilterPushedDown()) {
      return;
    }

    // When not, push those conditions down again
    LogicalExpression conditionExp = DrillOptiq.toDrill(new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner())), scan, condition);
    // Assembly conditions come out
    ElasticSearchFilterBuilder mongoFilterBuilder = new ElasticSearchFilterBuilder(groupScan, conditionExp);
    ElasticSearchScanSpec newScanSpec = mongoFilterBuilder.parseTree();
    if (newScanSpec == null) {
      return; // no filter pushdown so nothing to apply.
    }

    // Pass the filter in, and have scanned the data based on this
    ElasticSearchGroupScan newGroupsScan = null;

    newGroupsScan = new ElasticSearchGroupScan(groupScan.getUserName(), groupScan.getStoragePlugin(), newScanSpec, groupScan.getColumns());

    // Predicate pushdown completed
    newGroupsScan.setFilterPushedDown(true);

    final ScanPrel newScanPrel = ScanPrel.create(scan, filter.getTraitSet(), newGroupsScan, scan.getRowType());
    if (mongoFilterBuilder.isAllExpressionsConverted()) {
      /*
       * Since we could convert the entire filter condition expression into an
       * Mongo filter, we can eliminate the filter operator altogether.
       */
      // Indicates successful conversion into this expression
      call.transformTo(newScanPrel);
    } else {
      // Since some filters are not pushed down completely, copy it here
      call.transformTo(filter.copy(filter.getTraitSet(), ImmutableList.of((RelNode) newScanPrel)));
    }

  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final ScanPrel scan = (ScanPrel) call.rel(1);
    if (scan.getGroupScan() instanceof ElasticSearchGroupScan) {
      return super.matches(call);
    }
    return false;
  }
}
