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

import java.util.Arrays;
import java.util.Iterator;
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
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexLocalRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexShuttle;
import org.eigenbase.rex.RexVisitorImpl;

import com.carrotsearch.hppc.IntIntOpenHashMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
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

  public static Iterator<Prel> iter(RelNode... nodes){
    return (Iterator<Prel>) (Object) Arrays.asList(nodes).iterator();
  }

  public static Iterator<Prel> iter(List<RelNode> nodes) {
    return (Iterator<Prel>) (Object) nodes.iterator();
  }

  public static PlannerSettings getSettings(RelOptCluster cluster){
    return cluster.getPlanner().getContext().unwrap(PlannerSettings.class);
  }

  public static PlannerSettings getPlannerSettings(RelOptPlanner planner) {
    return planner.getContext().unwrap(PlannerSettings.class);
  }

  public static Prel removeSvIfRequired(Prel prel, SelectionVectorMode... allowed){
    SelectionVectorMode current = prel.getEncoding();
    for(SelectionVectorMode m : allowed){
      if(current == m) return prel;
    }
    return new SelectionVectorRemoverPrel(prel);
  }

  public static ProjectPushInfo getColumns(RelDataType rowType, List<RexNode> projects) {
    final List<String> fieldNames = rowType.getFieldNames();
    if (fieldNames.isEmpty()) return null;

    RefFieldsVisitor v = new RefFieldsVisitor(rowType);
    for (RexNode exp : projects) {
      PathSegment segment = exp.accept(v);
      v.addColumn(segment);
    }

    return v.getInfo();

  }

  public static class DesiredField {
    public final int origIndex;
    public final String name;
    public final RelDataTypeField field;

    public DesiredField(int origIndex, String name, RelDataTypeField field) {
      super();
      this.origIndex = origIndex;
      this.name = name;
      this.field = field;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((field == null) ? 0 : field.hashCode());
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + origIndex;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      DesiredField other = (DesiredField) obj;
      if (field == null) {
        if (other.field != null)
          return false;
      } else if (!field.equals(other.field))
        return false;
      if (name == null) {
        if (other.name != null)
          return false;
      } else if (!name.equals(other.name))
        return false;
      if (origIndex != other.origIndex)
        return false;
      return true;
    }

  }


  public static class ProjectPushInfo {
    public final List<SchemaPath> columns;
    public final List<DesiredField> desiredFields;
    public final InputRewriter rewriter;
    private final List<String> fieldNames;
    private final List<RelDataType> types;

    public ProjectPushInfo(List<SchemaPath> columns, ImmutableList<DesiredField> desiredFields) {
      super();
      this.columns = columns;
      this.desiredFields = desiredFields;

      this.fieldNames = Lists.newArrayListWithCapacity(desiredFields.size());
      this.types = Lists.newArrayListWithCapacity(desiredFields.size());
      IntIntOpenHashMap oldToNewIds = new IntIntOpenHashMap();

      int i =0;
      for(DesiredField f : desiredFields){
        fieldNames.add(f.name);
        types.add(f.field.getType());
        oldToNewIds.put(f.origIndex, i);
        i++;
      }
      this.rewriter = new InputRewriter(oldToNewIds);
    }

    public InputRewriter getInputRewriter(){
      return rewriter;
    }

    public boolean isStarQuery() {
      for (SchemaPath column : columns) {
        if (column.getRootSegment().getPath().startsWith("*")) {
          return true;
        }
      }
      return false;
    }

    public RelDataType createNewRowType(RelDataTypeFactory factory) {
      return factory.createStructType(types, fieldNames);
    }
  }

  /** Visitor that finds the set of inputs that are used. */
  private static class RefFieldsVisitor extends RexVisitorImpl<PathSegment> {
    final Set<SchemaPath> columns = Sets.newLinkedHashSet();
    final private List<String> fieldNames;
    final private List<RelDataTypeField> fields;
    final private Set<DesiredField> desiredFields = Sets.newHashSet();

    public RefFieldsVisitor(RelDataType rowType) {
      super(true);
      this.fieldNames = rowType.getFieldNames();
      this.fields = rowType.getFieldList();
    }

    public void addColumn(PathSegment segment) {
      if (segment != null && segment instanceof NameSegment) {
        columns.add(new SchemaPath((NameSegment)segment));
      }
    }

    public ProjectPushInfo getInfo(){
      return new ProjectPushInfo(ImmutableList.copyOf(columns), ImmutableList.copyOf(desiredFields));
    }


    @Override
    public PathSegment visitInputRef(RexInputRef inputRef) {
      int index = inputRef.getIndex();
      String name = fieldNames.get(index);
      RelDataTypeField field = fields.get(index);
      DesiredField f = new DesiredField(index, name, field);
      desiredFields.add(f);
      return new NameSegment(name);
    }

    @Override
    public PathSegment visitCall(RexCall call) {
      if ("ITEM".equals(call.getOperator().getName())) {
        PathSegment mapOrArray = call.operands.get(0).accept(this);
        if (mapOrArray != null) {
          if (call.operands.get(1) instanceof RexLiteral) {
            return mapOrArray.cloneWithNewChild(convertLiteral((RexLiteral) call.operands.get(1)));
          }
          return mapOrArray;
        }
      } else {
        for (RexNode operand : call.operands) {
          addColumn(operand.accept(this));
        }
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

  public static RelTraitSet fixTraits(RelOptRuleCall call, RelTraitSet set){
    return fixTraits(call.getPlanner(), set);
  }

  public static RelTraitSet fixTraits(RelOptPlanner cluster, RelTraitSet set){
    if(getPlannerSettings(cluster).isSingleMode()){
      return set.replace(DrillDistributionTrait.ANY);
    }else{
      return set;
    }
  }

  public static class InputRefRemap {
    private int oldIndex;
    private int newIndex;

    public InputRefRemap(int oldIndex, int newIndex) {
      super();
      this.oldIndex = oldIndex;
      this.newIndex = newIndex;
    }
    public int getOldIndex() {
      return oldIndex;
    }
    public int getNewIndex() {
      return newIndex;
    }


  }


  public static class InputRewriter extends RexShuttle {

    final IntIntOpenHashMap map;

    public InputRewriter(IntIntOpenHashMap map) {
      super();
      this.map = map;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      return new RexInputRef(map.get(inputRef.getIndex()), inputRef.getType());
    }

    @Override
    public RexNode visitLocalRef(RexLocalRef localRef) {
      return new RexInputRef(map.get(localRef.getIndex()), localRef.getType());
    }

  }
}
