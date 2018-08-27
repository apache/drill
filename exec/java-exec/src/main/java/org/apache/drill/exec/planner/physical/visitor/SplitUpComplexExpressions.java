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
package org.apache.drill.exec.planner.physical.visitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelConversionException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.sql.DrillOperatorTable;
import org.apache.drill.exec.planner.types.RelDataTypeDrillImpl;
import org.apache.drill.exec.planner.types.RelDataTypeHolder;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

public class SplitUpComplexExpressions extends BasePrelVisitor<Prel, Object, RelConversionException> {

  RelDataTypeFactory factory;
  DrillOperatorTable table;
  FunctionImplementationRegistry funcReg;

  public SplitUpComplexExpressions(RelDataTypeFactory factory, DrillOperatorTable table, FunctionImplementationRegistry funcReg) {
    super();
    this.factory = factory;
    this.table = table;
    this.funcReg = funcReg;
  }

  @Override
  public Prel visitPrel(Prel prel, Object value) throws RelConversionException {
    List<RelNode> children = Lists.newArrayList();
    for(Prel child : prel){
      child = child.accept(this, null);
      children.add(child);
    }
    return (Prel) prel.copy(prel.getTraitSet(), children);
  }


  @Override
  public Prel visitProject(ProjectPrel project, Object unused) throws RelConversionException {

    // Apply the rule to the child
    RelNode originalInput = ((Prel)project.getInput(0)).accept(this, null);
    project = (ProjectPrel) project.copy(project.getTraitSet(), Lists.newArrayList(originalInput));

    List<RexNode> exprList = new ArrayList<>();

    List<RelDataTypeField> relDataTypes = new ArrayList<>();
    List<RelDataTypeField> origRelDataTypes = new ArrayList<>();
    int i = 0;
    final int lastColumnReferenced = PrelUtil.getLastUsedColumnReference(project.getProjects());

    if (lastColumnReferenced == -1) {
      return project;
    }

    final int lastRexInput = lastColumnReferenced + 1;
    RexVisitorComplexExprSplitter exprSplitter = new RexVisitorComplexExprSplitter(factory, funcReg, lastRexInput);

    for (RexNode rex : project.getChildExps()) {
      origRelDataTypes.add(project.getRowType().getFieldList().get(i));
      i++;
      exprList.add(rex.accept(exprSplitter));
    }
    List<RexNode> complexExprs = exprSplitter.getComplexExprs();

    if (complexExprs.size() == 1 && findTopComplexFunc(project.getChildExps()).size() == 1) {
      return project;
    }

    ProjectPrel childProject;

    List<RexNode> allExprs = new ArrayList<>();
    int exprIndex = 0;
    List<String> fieldNames = originalInput.getRowType().getFieldNames();
    for (int index = 0; index < lastRexInput; index++) {
      RexBuilder builder = new RexBuilder(factory);
      allExprs.add(builder.makeInputRef( new RelDataTypeDrillImpl(new RelDataTypeHolder(), factory), index));

      if (fieldNames.get(index).contains(SchemaPath.DYNAMIC_STAR)) {
        relDataTypes.add(new RelDataTypeFieldImpl(fieldNames.get(index), allExprs.size(), factory.createSqlType(SqlTypeName.ANY)));
      } else {
        relDataTypes.add(new RelDataTypeFieldImpl("EXPR$" + exprIndex, allExprs.size(), factory.createSqlType(SqlTypeName.ANY)));
        exprIndex++;
      }
    }
    RexNode currRexNode;
    int index = lastRexInput - 1;

    // if the projection expressions contained complex outputs, split them into their own individual projects
    if (complexExprs.size() > 0 ) {
      while (complexExprs.size() > 0) {
        if ( index >= lastRexInput ) {
          allExprs.remove(allExprs.size() - 1);
          RexBuilder builder = new RexBuilder(factory);
          allExprs.add(builder.makeInputRef( new RelDataTypeDrillImpl(new RelDataTypeHolder(), factory), index));
        }
        index++;
        exprIndex++;

        currRexNode = complexExprs.remove(0);
        allExprs.add(currRexNode);
        relDataTypes.add(new RelDataTypeFieldImpl("EXPR$" + exprIndex, allExprs.size(), factory.createSqlType(SqlTypeName.ANY)));
        childProject = new ProjectPrel(project.getCluster(), project.getTraitSet(), originalInput, ImmutableList.copyOf(allExprs), new RelRecordType(relDataTypes));
        originalInput = childProject;
      }
      // copied from above, find a better way to do this
      allExprs.remove(allExprs.size() - 1);
      RexBuilder builder = new RexBuilder(factory);
      allExprs.add(builder.makeInputRef( new RelDataTypeDrillImpl(new RelDataTypeHolder(), factory), index));
      relDataTypes.add(new RelDataTypeFieldImpl("EXPR$" + index, allExprs.size(), factory.createSqlType(SqlTypeName.ANY) ));
    }
    return (Prel) project.copy(project.getTraitSet(), originalInput, exprList, new RelRecordType(origRelDataTypes));
  }

  /**
   *  Find the list of expressions where Complex type function is at top level.
   */
  private List<RexNode> findTopComplexFunc(List<RexNode> exprs) {
    final List<RexNode> topComplexFuncs = new ArrayList<>();

    for (RexNode exp : exprs) {
      if (exp instanceof RexCall) {
        RexCall call = (RexCall) exp;
        String functionName = call.getOperator().getName();

        if (funcReg.isFunctionComplexOutput(functionName) ) {
          topComplexFuncs.add(exp);
        }
      }
    }

    return topComplexFuncs;
  }

}
