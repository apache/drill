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
import java.util.Set;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.PathSegment.ArraySegment;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.SelectionVectorRemover;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexOver;
import org.eigenbase.rex.RexVisitorImpl;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

public class PrelUtil {

  public static List<Ordering> getOrdering(RelCollation collation, RelDataType rowType) {
    List<Ordering> orderExpr = Lists.newArrayList();

    final List<String> childFields = rowType.getFieldNames();

    for (RelFieldCollation fc: collation.getFieldCollations() ) {
      FieldReference fr = new FieldReference(childFields.get(fc.getFieldIndex()), ExpressionPosition.UNKNOWN);
      orderExpr.add(new Ordering(fc.getDirection(), fr, fc.nullDirection));
    }

    return orderExpr;
  }

  /*
   * Return a hash expression :  hash(field1) ^ hash(field2) ^ hash(field3) ... ^ hash(field_n)
   */
  public static LogicalExpression getHashExpression(List<DistributionField> fields, RelDataType rowType) {
    assert fields.size() > 0;

    final List<String> childFields = rowType.getFieldNames();

    FieldReference fr = new FieldReference(childFields.get(fields.get(0).getFieldId()), ExpressionPosition.UNKNOWN);
    FunctionCall func = new FunctionCall("hash",  ImmutableList.of((LogicalExpression)fr), ExpressionPosition.UNKNOWN);

    for (int i = 1; i<fields.size(); i++) {
      fr = new FieldReference(childFields.get(fields.get(i).getFieldId()), ExpressionPosition.UNKNOWN);
      FunctionCall func2 = new FunctionCall("hash",  ImmutableList.of((LogicalExpression)fr), ExpressionPosition.UNKNOWN);

      func = new FunctionCall("xor", ImmutableList.of((LogicalExpression)func, (LogicalExpression)func2), ExpressionPosition.UNKNOWN);
    }

    return func;
  }

  public static PlannerSettings getSettings(RelOptCluster cluster){
    return cluster.getPlanner().getFrameworkContext().unwrap(PlannerSettings.class);
  }

  public static PhysicalOperator removeSvIfRequired(PhysicalOperator child, SelectionVectorMode... allowed){
    SelectionVectorMode current = child.getSVMode();
    for(SelectionVectorMode m : allowed){
      if(current == m) return child;
    }
    return new SelectionVectorRemover(child);
  }

  public static List<SchemaPath> getColumns(RelDataType rowType, List<RexNode> projects) {
    final List<String> fieldNames = rowType.getFieldNames();
    if (fieldNames.isEmpty()) return ImmutableList.of();

    RefFieldsVisitor v = new RefFieldsVisitor(fieldNames);
    for (RexNode exp : projects) {
      PathSegment segment = exp.accept(v);
      v.addColumn(segment);
    }

    List<SchemaPath> columns = v.getColumns();
    for (SchemaPath column : columns) {
      if (column.getRootSegment().getPath().startsWith("*")) {
        return ImmutableList.of();
      }
    }

    return columns;
  }

  /** Visitor that finds the set of inputs that are used. */
  private static class RefFieldsVisitor extends RexVisitorImpl<PathSegment> {
    final Set<SchemaPath> columns = Sets.newLinkedHashSet();
    final private List<String> fieldNames;

    public RefFieldsVisitor(List<String> fieldNames) {
      super(true);
      this.fieldNames = fieldNames;
    }

    public void addColumn(PathSegment segment) {
      if (segment != null && segment instanceof NameSegment) {
        columns.add(new SchemaPath((NameSegment)segment));
      }
    }

    public List<SchemaPath> getColumns() {
      return ImmutableList.copyOf(columns);
    }

    @Override
    public PathSegment visitInputRef(RexInputRef inputRef) {
      return new NameSegment(fieldNames.get(inputRef.getIndex()));
    }

    @Override
    public PathSegment visitCall(RexCall call) {
      if ("ITEM".equals(call.getOperator().getName())) {
        return call.operands.get(0).accept(this)
            .cloneWithNewChild(convertLiteral((RexLiteral) call.operands.get(1)));
      }
      // else
      for (RexNode operand : call.operands) {
        addColumn(operand.accept(this));
      }
      return null;
    }

    private PathSegment convertLiteral(RexLiteral literal) {
      switch (literal.getType().getSqlTypeName()) {
      case CHAR:
        return new NameSegment(RexLiteral.stringValue(literal));
      case INTEGER:
        return new ArraySegment(RexLiteral.intValue(literal));
      default:
        return null;
      }
    }

  }

}
