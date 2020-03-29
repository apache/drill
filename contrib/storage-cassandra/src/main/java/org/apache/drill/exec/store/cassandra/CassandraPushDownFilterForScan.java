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
package org.apache.drill.exec.store.cassandra;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexNode;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraPushDownFilterForScan extends StoragePluginOptimizerRule {

  private static final Logger logger = LoggerFactory.getLogger(CassandraDatabaseSchema.class);

  public static final StoragePluginOptimizerRule INSTANCE = new CassandraPushDownFilterForScan();

  private CassandraPushDownFilterForScan() {

    super(RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)), "CassandraPushDownFilterForScan");
    //super(RelOptHelper.some(CassandraRel.class, RelOptHelper.any(ScanPrel.class)), "CassandraPushDownFilterForScan");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ScanPrel scan = (ScanPrel) call.rel(1);
    final FilterPrel filter = (FilterPrel) call.rel(0);
    final RexNode condition = filter.getCondition();

    CassandraGroupScan groupScan = (CassandraGroupScan)scan.getGroupScan();

    // TODO This is always true, hence filters NEVER get pushed down
    if (groupScan.isFilterPushedDown()) {

      /*
       * The rule can get triggered again due to the transformed "scan => filter" sequence
       * created by the earlier execution of this rule when we could not do a complete
       * conversion of Optiq Filter's condition to Cassandra Filter. In such cases, we rely upon
       * this flag to not do a re-processing of the rule on the already transformed call.
       */
      return;
    }

    LogicalExpression conditionExp = DrillOptiq.toDrill(new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner())), scan, condition);
    CassandraFilterBuilder cassandraFilterBuilder = new CassandraFilterBuilder(groupScan, conditionExp);
    CassandraScanSpec newScanSpec = cassandraFilterBuilder.parseTree();

    if (newScanSpec == null) {
      return; //no filter pushdown ==> No transformation.
    }

    final CassandraGroupScan newGroupScan = new CassandraGroupScan(groupScan.getUserName(),
      groupScan.getStoragePlugin(),
      newScanSpec,
      groupScan.getColumns());
    newGroupScan.setFilterPushedDown(true);

    final ScanPrel newScanPrel = ScanPrel.create(scan, filter.getTraitSet(), newGroupScan, scan.getRowType());
    if (cassandraFilterBuilder.isAllExpressionsConverted()) {
      /*
       * Since we could convert the entire filter condition expression into an cassandra filter,
       * we can eliminate the filter operator altogether.
       */
      call.transformTo(newScanPrel);
    } else {
      call.transformTo(filter.copy(filter.getTraitSet(), ImmutableList.of((RelNode)newScanPrel)));
    }
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final ScanPrel scan = (ScanPrel) call.rel(1);
    if (scan.getGroupScan() instanceof CassandraGroupScan) {
      return super.matches(call);
    }
    return false;
  }
}
