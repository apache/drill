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
package org.apache.drill.exec.planner.physical;

import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;

public class ScanPrule extends RelOptRule{
  public static final RelOptRule INSTANCE = new ScanPrule();

  public ScanPrule() {
    super(RelOptHelper.any(DrillScanRel.class), "Prel.ScanPrule");

  }
  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillScanRel scan = (DrillScanRel) call.rel(0);

    List<SchemaPath> columns = PrelUtil.getColumns(scan.getRowType());
    columns = columns == null || columns.isEmpty() ? null : columns;

    GroupScan groupScan = scan.getGroupScan().clone(columns);

    DrillDistributionTrait partition = groupScan.getMaxParallelizationWidth() > 1 ? DrillDistributionTrait.RANDOM_DISTRIBUTED : DrillDistributionTrait.SINGLETON;

    final RelTraitSet traits = scan.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(partition);

    final DrillScanPrel newScan = ScanPrel.create(scan, traits, groupScan, scan.getRowType());

    call.transformTo(newScan);
  }

}
