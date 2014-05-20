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

import java.util.Iterator;
import java.util.List;

import org.apache.drill.exec.planner.common.DrillJoinRelBase;
import org.apache.drill.exec.planner.physical.visitor.PrelVisitor;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;

import com.google.common.collect.Lists;

/**
 *
 * Base class for MergeJoinPrel and HashJoinPrel
 *
 */
public abstract class JoinPrel extends DrillJoinRelBase implements Prel{

  public JoinPrel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
      JoinRelType joinType) throws InvalidRelException{
    super(cluster, traits, left, right, condition, joinType);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitJoin(this, value);
  }

  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getLeft(), getRight());
  }

  /**
   * Check to make sure that the fields of the inputs are the same as the output field names.  If not, insert a project renaming them.
   * @param implementor
   * @param i
   * @param offset
   * @param input
   * @return
   */
  public RelNode getJoinInput(int offset, RelNode input) {
    assert uniqueFieldNames(input.getRowType());
    final List<String> fields = getRowType().getFieldNames();
    final List<String> inputFields = input.getRowType().getFieldNames();
    final List<String> outputFields = fields.subList(offset, offset + inputFields.size());
    if (!outputFields.equals(inputFields)) {
      // Ensure that input field names are the same as output field names.
      // If there are duplicate field names on left and right, fields will get
      // lost.
      // In such case, we need insert a rename Project on top of the input.
      return rename(input, input.getRowType().getFieldList(), outputFields);
    } else {
      return input;
    }
  }

  private RelNode rename(RelNode input, List<RelDataTypeField> inputFields, List<String> outputFieldNames) {
    List<RexNode> exprs = Lists.newArrayList();

    for (RelDataTypeField field : inputFields) {
      RexNode expr = input.getCluster().getRexBuilder().makeInputRef(field.getType(), field.getIndex());
      exprs.add(expr);
    }

    RelDataType rowType = RexUtil.createStructType(input.getCluster().getTypeFactory(), exprs, outputFieldNames);

    ProjectPrel proj = new ProjectPrel(input.getCluster(), input.getTraitSet(), input, exprs, rowType);

    return proj;
  }

}
