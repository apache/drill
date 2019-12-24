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
 * 这是一个优化器
 *
 */
public class MongoPushDownFilterForScan extends StoragePluginOptimizerRule {
  private static final Logger logger = LoggerFactory
      .getLogger(MongoPushDownFilterForScan.class);
  public static final StoragePluginOptimizerRule INSTANCE = new MongoPushDownFilterForScan();

  private MongoPushDownFilterForScan() {
    super(
        RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)),
        "MongoPushDownFilterForScan");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ScanPrel scan = (ScanPrel) call.rel(1);
    final FilterPrel filter = (FilterPrel) call.rel(0);
    final RexNode condition = filter.getCondition();

    ElasticSearchGroupScan groupScan = (ElasticSearchGroupScan) scan.getGroupScan();
    // 是否已经下推了
    if (groupScan.isFilterPushedDown()) {
      return;
    }

    // 当没有时，就把这些条件重新下推一下
    LogicalExpression conditionExp = DrillOptiq.toDrill(
        new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner())), scan, condition);
    // 组装条件出来
    ElasticSearchFilterBuilder mongoFilterBuilder = new ElasticSearchFilterBuilder(groupScan,
        conditionExp);
    ElasticSearchScanSpec newScanSpec = mongoFilterBuilder.parseTree();
    if (newScanSpec == null) {
      return; // no filter pushdown so nothing to apply.
    }

    //把filter传了进去，已经基于这里进行数据的扫描了
    ElasticSearchGroupScan newGroupsScan = null;
   
      newGroupsScan = new ElasticSearchGroupScan(groupScan.getUserName(), groupScan.getStoragePlugin(),
          newScanSpec, groupScan.getColumns());
  
    // 已经完成谓词下推了
    newGroupsScan.setFilterPushedDown(true);

    final ScanPrel newScanPrel = ScanPrel.create(scan, filter.getTraitSet(),
        newGroupsScan, scan.getRowType());
    if (mongoFilterBuilder.isAllExpressionsConverted()) {
      /*
       * Since we could convert the entire filter condition expression into an
       * Mongo filter, we can eliminate the filter operator altogether.
       */
    	// 表示进行成功转换成这个表达式了
      call.transformTo(newScanPrel);
    } else {
    	// 由于有些fielter没有完全下推的了，所以在这里要复制一下
      call.transformTo(filter.copy(filter.getTraitSet(),
          ImmutableList.of((RelNode) newScanPrel)));
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
