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
package org.apache.drill.exec.store.accumulo;

import java.util.List;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.DrillSortRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optimizer rule that pushes sort operations into Accumulo scans.
 *
 * <p>Accumulo naturally returns rows sorted by row key in ascending order.
 * This rule detects when a sort is on the row_key column and can be satisfied
 * by Accumulo's natural ordering:</p>
 *
 * <ul>
 *   <li>ASC order on row_key: Use Scanner (maintains order) - data is naturally sorted</li>
 *   <li>DESC order on row_key: Configure scanner for reverse iteration</li>
 * </ul>
 *
 * <p>When sort is pushed down, the sort operator may be eliminated by
 * subsequent optimization passes since the data is already sorted.</p>
 */
public abstract class AccumuloPushSortIntoScan extends StoragePluginOptimizerRule {
  private static final Logger logger = LoggerFactory.getLogger(AccumuloPushSortIntoScan.class);

  private AccumuloPushSortIntoScan(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  /**
   * Rule for Sort directly on Scan.
   */
  public static final StoragePluginOptimizerRule SORT_ON_SCAN =
      new AccumuloPushSortIntoScan(
          RelOptHelper.some(DrillSortRel.class, RelOptHelper.any(DrillScanRel.class)),
          "AccumuloPushSortIntoScan:Sort_On_Scan") {

    @Override
    public void onMatch(RelOptRuleCall call) {
      DrillSortRel sort = call.rel(0);
      DrillScanRel scan = call.rel(1);
      doPushSortIntoScan(call, sort, scan);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      DrillScanRel scan = call.rel(1);
      if (!(scan.getGroupScan() instanceof AccumuloGroupScan)) {
        return false;
      }
      AccumuloGroupScan groupScan = (AccumuloGroupScan) scan.getGroupScan();
      // Don't push sort if already pushed
      return !groupScan.isSortPushedDown();
    }
  };

  /**
   * Pushes sort into Accumulo scan if the sort is on row_key.
   */
  protected void doPushSortIntoScan(RelOptRuleCall call, DrillSortRel sort, DrillScanRel scan) {
    AccumuloGroupScan groupScan = (AccumuloGroupScan) scan.getGroupScan();

    // Check if sort is on row_key
    RelCollation collation = sort.getCollation();
    List<RelFieldCollation> fieldCollations = collation.getFieldCollations();

    // We only support single-column sort on row_key for now
    if (fieldCollations.size() != 1) {
      logger.debug("Sort has {} fields, only single-column sort on row_key is supported",
          fieldCollations.size());
      return;
    }

    RelFieldCollation fieldCollation = fieldCollations.get(0);
    int fieldIndex = fieldCollation.getFieldIndex();

    // row_key is always at index 0 in our schema
    if (fieldIndex != 0) {
      logger.debug("Sort field index {} is not row_key (index 0)", fieldIndex);
      return;
    }

    // Check the sort direction
    Direction direction = fieldCollation.getDirection();
    boolean isDescending = (direction == Direction.DESCENDING || direction == Direction.STRICTLY_DESCENDING);

    // Create new scan spec with sort direction
    AccumuloScanSpec newScanSpec = groupScan.getScanSpec().withSortOrder(isDescending);
    AccumuloGroupScan newGroupScan = groupScan.cloneWithNewScanSpec(newScanSpec);
    newGroupScan.setSortPushedDown(true);

    // Create new scan with the updated group scan
    DrillScanRel newScan = new DrillScanRel(
        scan.getCluster(),
        scan.getTraitSet(),
        scan.getTable(),
        newGroupScan,
        scan.getRowType(),
        scan.getColumns(),
        scan.partitionFilterPushdown());

    // Keep the sort but with the underlying scan now using sorted iteration
    // The sort may be removed by later optimization passes if the data is already sorted
    DrillSortRel newSort = new DrillSortRel(
        sort.getCluster(),
        sort.getTraitSet(),
        newScan,
        sort.getCollation(),
        sort.offset,
        sort.fetch);

    call.transformTo(newSort);
    logger.debug("Pushed {} sort into Accumulo scan for table {}",
        isDescending ? "DESC" : "ASC", groupScan.getTableName());
  }
}
