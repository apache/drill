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

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import net.hydromatic.linq4j.Ord;
import net.hydromatic.optiq.util.BitSets;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillWindowRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.WindowRelBase;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.reltype.RelRecordType;
import org.eigenbase.sql.SqlAggFunction;

import java.util.List;

public class StreamingWindowPrule extends RelOptRule {
  public static final RelOptRule INSTANCE = new StreamingWindowPrule();

  private StreamingWindowPrule() {
    super(RelOptHelper.some(DrillWindowRel.class, DrillRel.DRILL_LOGICAL, RelOptHelper.any(RelNode.class)), "Prel.WindowPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillWindowRel window = call.rel(0);
    RelNode input = call.rel(1);

    // TODO: Order window based on existing partition by
    //input.getTraitSet().subsumes()

    for (final Ord<WindowRelBase.Window> w : Ord.zip(window.windows)) {
      WindowRelBase.Window windowBase = w.getValue();
      DrillDistributionTrait distOnAllKeys =
          new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED,
              ImmutableList.copyOf(getDistributionFields(windowBase)));

      RelCollation collation = getCollation(windowBase);
      RelTraitSet traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(collation).plus(distOnAllKeys);
      final RelNode convertedInput = convert(input, traits);

      List<RelDataTypeField> newRowFields = Lists.newArrayList();
      for(RelDataTypeField field : convertedInput.getRowType().getFieldList()) {
        newRowFields.add(field);
      }

      Iterable<RelDataTypeField> newWindowFields = Iterables.filter(window.getRowType().getFieldList(), new Predicate<RelDataTypeField>() {
            @Override
            public boolean apply(RelDataTypeField relDataTypeField) {
              return relDataTypeField.getName().startsWith("w" + w.i + "$");
            }
      });

      for(RelDataTypeField newField : newWindowFields) {
        newRowFields.add(newField);
      }

      RelDataType rowType = new RelRecordType(newRowFields);

      List<WindowRelBase.RexWinAggCall> newWinAggCalls = Lists.newArrayList();
      for(Ord<WindowRelBase.RexWinAggCall> aggOrd : Ord.zip(windowBase.aggCalls)) {
        WindowRelBase.RexWinAggCall aggCall = aggOrd.getValue();
        newWinAggCalls.add(new WindowRelBase.RexWinAggCall(
            (SqlAggFunction)aggCall.getOperator(), aggCall.getType(), aggCall.getOperands(), aggOrd.i)
        );
      }

      windowBase = new WindowRelBase.Window(
          windowBase.groupSet,
          windowBase.isRows,
          windowBase.lowerBound,
          windowBase.upperBound,
          windowBase.orderKeys,
          newWinAggCalls
      );

      input = new StreamingWindowPrel(
          window.getCluster(),
          window.getTraitSet().merge(traits),
          convertedInput,
          window.getConstants(),
          rowType,
          windowBase);
    }

    call.transformTo(input);
  }

  private RelCollation getCollation(WindowRelBase.Window window) {
    List<RelFieldCollation> fields = Lists.newArrayList();
    for (int group : BitSets.toIter(window.groupSet)) {
      fields.add(new RelFieldCollation(group));
    }
    return RelCollationImpl.of(fields);
  }

  private List<DrillDistributionTrait.DistributionField> getDistributionFields(WindowRelBase.Window window) {
    List<DrillDistributionTrait.DistributionField> groupByFields = Lists.newArrayList();
    for (int group : BitSets.toIter(window.groupSet)) {
      DrillDistributionTrait.DistributionField field = new DrillDistributionTrait.DistributionField(group);
      groupByFields.add(field);
    }
    return groupByFields;
  }
}
