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
package org.apache.drill.exec.planner.logical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Project;
import org.apache.drill.exec.planner.torel.ConversionContext;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.reltype.RelDataTypeFieldImpl;
import org.eigenbase.reltype.RelRecordType;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.Pair;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;

/**
 * Project implemented in Drill.
 */
public class DrillProjectRel extends ProjectRelBase implements DrillRel {
  protected DrillProjectRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, List<RexNode> exps,
      RelDataType rowType) {
    super(cluster, traits, child, exps, rowType, Flags.BOXED);
    assert getConvention() == CONVENTION;
  }


  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DrillProjectRel(getCluster(), traitSet, sole(inputs), new ArrayList<RexNode>(exps), rowType);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(0.1);
  }

  private List<Pair<RexNode, String>> projects() {
    return Pair.zip(exps, getRowType().getFieldNames());
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    LogicalOperator inputOp = implementor.visitChild(this, 0, getChild());
    Project.Builder builder = Project.builder();
    builder.setInput(inputOp);
    for (Pair<RexNode, String> pair : projects()) {
      LogicalExpression expr = DrillOptiq.toDrill(implementor.getContext(), getChild(), pair.left);
      builder.addExpr(new FieldReference("output." + pair.right), expr);
    }
    return builder.build();
  }
  
  public static DrillProjectRel convert(Project project, ConversionContext context) throws InvalidRelException{
    RelNode input = context.toRel(project.getInput());
    List<RelDataTypeField> fields = Lists.newArrayList();
    List<RexNode> exps = Lists.newArrayList();
    for(NamedExpression expr : project.getSelections()){
      fields.add(new RelDataTypeFieldImpl(expr.getRef().getPath().toString(), fields.size(), context.getTypeFactory().createSqlType(SqlTypeName.ANY) ));
      exps.add(context.toRex(expr.getExpr()));
    }
    return new DrillProjectRel(context.getCluster(), context.getLogicalTraits(), input, exps, new RelRecordType(fields));
  }

}
