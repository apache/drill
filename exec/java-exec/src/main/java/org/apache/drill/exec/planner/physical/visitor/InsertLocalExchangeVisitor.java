/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.physical.visitor;

import com.google.common.collect.Lists;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.planner.physical.ExchangePrel;
import org.apache.drill.exec.planner.physical.HashPrelUtil;
import org.apache.drill.exec.planner.physical.HashPrelUtil.HashExpressionCreatorHelper;
import org.apache.drill.exec.planner.physical.HashToRandomExchangePrel;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;
import org.apache.drill.exec.planner.physical.UnorderedDeMuxExchangePrel;
import org.apache.drill.exec.planner.physical.UnorderedMuxExchangePrel;
import org.apache.drill.exec.planner.sql.DrillSqlOperator;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.Collections;
import java.util.List;

public class InsertLocalExchangeVisitor extends BasePrelVisitor<Prel, Void, RuntimeException> {
  private final boolean isMuxEnabled;
  private final boolean isDeMuxEnabled;


  public static class RexNodeBasedHashExpressionCreatorHelper implements HashExpressionCreatorHelper<RexNode> {
    private final RexBuilder rexBuilder;

    public RexNodeBasedHashExpressionCreatorHelper(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode createCall(String funcName, List<RexNode> inputFields) {
      final DrillSqlOperator op =
          new DrillSqlOperator(funcName, inputFields.size(), true);
      return rexBuilder.makeCall(op, inputFields);
    }
  }

  public static Prel insertLocalExchanges(Prel prel, OptionManager options) {
    boolean isMuxEnabled = options.getOption(PlannerSettings.MUX_EXCHANGE.getOptionName()).bool_val;
    boolean isDeMuxEnabled = options.getOption(PlannerSettings.DEMUX_EXCHANGE.getOptionName()).bool_val;

    if (isMuxEnabled || isDeMuxEnabled) {
      return prel.accept(new InsertLocalExchangeVisitor(isMuxEnabled, isDeMuxEnabled), null);
    }

    return prel;
  }

  public InsertLocalExchangeVisitor(boolean isMuxEnabled, boolean isDeMuxEnabled) {
    this.isMuxEnabled = isMuxEnabled;
    this.isDeMuxEnabled = isDeMuxEnabled;
  }

  @Override
  public Prel visitExchange(ExchangePrel prel, Void value) throws RuntimeException {
    Prel child = ((Prel)prel.getInput()).accept(this, null);
    // Whenever we encounter a HashToRandomExchangePrel
    //   If MuxExchange is enabled, insert a UnorderedMuxExchangePrel before HashToRandomExchangePrel.
    //   If DeMuxExchange is enabled, insert a UnorderedDeMuxExchangePrel after HashToRandomExchangePrel.
    if (!(prel instanceof HashToRandomExchangePrel)) {
      return (Prel)prel.copy(prel.getTraitSet(), Collections.singletonList(((RelNode)child)));
    }

    Prel newPrel = child;

    final HashToRandomExchangePrel hashPrel = (HashToRandomExchangePrel) prel;
    final List<String> childFields = child.getRowType().getFieldNames();

    List <RexNode> removeUpdatedExpr = null;

    if (isMuxEnabled) {
      // Insert Project Operator with new column that will be a hash for HashToRandomExchange fields
      final List<DistributionField> distFields = hashPrel.getFields();
      final List<String> outputFieldNames = Lists.newArrayList(childFields);
      final RexBuilder rexBuilder = prel.getCluster().getRexBuilder();
      final List<RelDataTypeField> childRowTypeFields = child.getRowType().getFieldList();

      final HashExpressionCreatorHelper<RexNode> hashHelper = new RexNodeBasedHashExpressionCreatorHelper(rexBuilder);
      final List<RexNode> distFieldRefs = Lists.newArrayListWithExpectedSize(distFields.size());
      for(int i=0; i<distFields.size(); i++) {
        final int fieldId = distFields.get(i).getFieldId();
        distFieldRefs.add(rexBuilder.makeInputRef(childRowTypeFields.get(fieldId).getType(), fieldId));
      }

      final List <RexNode> updatedExpr = Lists.newArrayListWithExpectedSize(childRowTypeFields.size());
      removeUpdatedExpr = Lists.newArrayListWithExpectedSize(childRowTypeFields.size());
      for ( RelDataTypeField field : childRowTypeFields) {
        RexNode rex = rexBuilder.makeInputRef(field.getType(), field.getIndex());
        updatedExpr.add(rex);
        removeUpdatedExpr.add(rex);
      }

      outputFieldNames.add(HashPrelUtil.HASH_EXPR_NAME);
      updatedExpr.add(HashPrelUtil.createHashBasedPartitionExpression(distFieldRefs, hashHelper));

      RelDataType rowType = RexUtil.createStructType(prel.getCluster().getTypeFactory(), updatedExpr, outputFieldNames);

      ProjectPrel addColumnprojectPrel = new ProjectPrel(child.getCluster(), child.getTraitSet(), child, updatedExpr, rowType);

      newPrel = new UnorderedMuxExchangePrel(addColumnprojectPrel.getCluster(), addColumnprojectPrel.getTraitSet(),
          addColumnprojectPrel);
    }

    newPrel = new HashToRandomExchangePrel(prel.getCluster(),
        prel.getTraitSet(), newPrel, ((HashToRandomExchangePrel) prel).getFields());

    if (isDeMuxEnabled) {
      HashToRandomExchangePrel hashExchangePrel = (HashToRandomExchangePrel) newPrel;
      // Insert a DeMuxExchange to narrow down the number of receivers
      newPrel = new UnorderedDeMuxExchangePrel(prel.getCluster(), prel.getTraitSet(), hashExchangePrel,
          hashExchangePrel.getFields());
    }

    if ( isMuxEnabled ) {
      // remove earlier inserted Project Operator - since it creates issues down the road in HashJoin
      RelDataType removeRowType = RexUtil.createStructType(newPrel.getCluster().getTypeFactory(), removeUpdatedExpr, childFields);

      ProjectPrel removeColumnProjectPrel = new ProjectPrel(newPrel.getCluster(), newPrel.getTraitSet(), newPrel, removeUpdatedExpr, removeRowType);
      return removeColumnProjectPrel;
    }
    return newPrel;
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
    List<RelNode> children = Lists.newArrayList();
    for(Prel child : prel){
      children.add(child.accept(this, null));
    }
    return (Prel) prel.copy(prel.getTraitSet(), children);
  }
}
