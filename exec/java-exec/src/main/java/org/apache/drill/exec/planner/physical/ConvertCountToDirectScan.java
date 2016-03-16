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

import java.util.Collections;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillAggregateRel;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.store.direct.DirectGroupScan;
import org.apache.drill.exec.store.pojo.PojoRecordReader;

import com.google.common.collect.Lists;

/**
 * This rule will convert
 *   " select count(*)  as mycount from table "
 * or " select count( not-nullable-expr) as mycount from table "
 *   into
 *
 *    Project(mycount)
 *         \
 *    DirectGroupScan ( PojoRecordReader ( rowCount ))
 *
 * or
 *    " select count(column) as mycount from table "
 *    into
 *      Project(mycount)
 *           \
 *            DirectGroupScan (PojoRecordReader (columnValueCount))
 *
 * Currently, only parquet group scan has the exact row count and column value count,
 * obtained from parquet row group info. This will save the cost to
 * scan the whole parquet files.
 */

public class ConvertCountToDirectScan extends Prule {

  public static final RelOptRule AGG_ON_PROJ_ON_SCAN = new ConvertCountToDirectScan(
      RelOptHelper.some(DrillAggregateRel.class,
                        RelOptHelper.some(DrillProjectRel.class,
                            RelOptHelper.any(DrillScanRel.class))), "Agg_on_proj_on_scan");

  public static final RelOptRule AGG_ON_SCAN = new ConvertCountToDirectScan(
      RelOptHelper.some(DrillAggregateRel.class,
                            RelOptHelper.any(DrillScanRel.class)), "Agg_on_scan");

  /** Creates a SplunkPushDownRule. */
  protected ConvertCountToDirectScan(RelOptRuleOperand rule, String id) {
    super(rule, "ConvertCountToDirectScan:" + id);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillAggregateRel agg = (DrillAggregateRel) call.rel(0);
    final DrillScanRel scan = (DrillScanRel) call.rel(call.rels.length -1);
    final DrillProjectRel proj = call.rels.length == 3 ? (DrillProjectRel) call.rel(1) : null;

    final GroupScan oldGrpScan = scan.getGroupScan();
    final PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());

    // Only apply the rule when :
    //    1) scan knows the exact row count in getSize() call,
    //    2) No GroupBY key,
    //    3) only one agg function (Check if it's count(*) below).
    //    4) No distinct agg call.
    if (!(oldGrpScan.getScanStats(settings).getGroupScanProperty().hasExactRowCount()
        && agg.getGroupCount() == 0
        && agg.getAggCallList().size() == 1
        && !agg.containsDistinctCall())) {
      return;
    }

    AggregateCall aggCall = agg.getAggCallList().get(0);

    if (aggCall.getAggregation().getName().equals("COUNT") ) {

      long cnt = 0;
      //  count(*)  == >  empty arg  ==>  rowCount
      //  count(Not-null-input) ==> rowCount
      if (aggCall.getArgList().isEmpty() ||
          (aggCall.getArgList().size() == 1 &&
           ! agg.getInput().getRowType().getFieldList().get(aggCall.getArgList().get(0).intValue()).getType().isNullable())) {
        cnt = (long) oldGrpScan.getScanStats(settings).getRecordCount();
      } else if (aggCall.getArgList().size() == 1) {
      // count(columnName) ==> Agg ( Scan )) ==> columnValueCount
        int index = aggCall.getArgList().get(0);

        if (proj != null) {
          // project in the middle of Agg and Scan : Only when input of AggCall is a RexInputRef in Project, we find the index of Scan's field.
          // For instance,
          // Agg - count($0)
          //  \
          //  Proj - Exp={$1}
          //    \
          //   Scan (col1, col2).
          // return count of "col2" in Scan's metadata, if found.

          if (proj.getProjects().get(index) instanceof RexInputRef) {
            index = ((RexInputRef) proj.getProjects().get(index)).getIndex();
          } else {
            return;  // do not apply for all other cases.
          }
        }

        String columnName = scan.getRowType().getFieldNames().get(index).toLowerCase();

        cnt = oldGrpScan.getColumnValueCount(SchemaPath.getSimplePath(columnName));
        if (cnt == GroupScan.NO_COLUMN_STATS) {
          // if column stats are not available don't apply this rule
          return;
        }
      } else {
        return; // do nothing.
      }

      RelDataType scanRowType = getCountDirectScanRowType(agg.getCluster().getTypeFactory());

      final ScanPrel newScan = ScanPrel.create(scan,
          scan.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON), getCountDirectScan(cnt),
          scanRowType);

      List<RexNode> exprs = Lists.newArrayList();
      exprs.add(RexInputRef.of(0, scanRowType));

      final ProjectPrel newProj = new ProjectPrel(agg.getCluster(), agg.getTraitSet().plus(Prel.DRILL_PHYSICAL)
          .plus(DrillDistributionTrait.SINGLETON), newScan, exprs, agg.getRowType());

      call.transformTo(newProj);
    }

  }

  /**
   * Class to represent the count aggregate result.
   */
  public static class CountQueryResult {
    public long count;

    public CountQueryResult(long cnt) {
      this.count = cnt;
    }
  }

  private RelDataType getCountDirectScanRowType(RelDataTypeFactory typeFactory) {
    List<RelDataTypeField> fields = Lists.newArrayList();
    fields.add(new RelDataTypeFieldImpl("count", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));

    return new RelRecordType(fields);
  }

  private GroupScan getCountDirectScan(long cnt) {
    CountQueryResult res = new CountQueryResult(cnt);

    PojoRecordReader<CountQueryResult> reader = new PojoRecordReader<CountQueryResult>(CountQueryResult.class,
        Collections.singleton(res).iterator());

    return new DirectGroupScan(reader);
  }

}
