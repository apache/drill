/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.planner.physical.visitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.types.RelDataTypeDrillImpl;
import org.apache.drill.exec.planner.types.RelDataTypeHolder;

public class RexVisitorComplexExprSplitter extends RexVisitorImpl<RexNode> {

  RelDataTypeFactory factory;
  FunctionImplementationRegistry funcReg;
  List<RexNode> complexExprs;
  List<ProjectPrel> projects;
  int lastUsedIndex;

  public RexVisitorComplexExprSplitter(RelDataTypeFactory factory, FunctionImplementationRegistry funcReg, int firstUnused) {
    super(true);
    this.factory = factory;
    this.funcReg = funcReg;
    this.complexExprs = new ArrayList<>();
    this.lastUsedIndex = firstUnused;
  }

  public  List<RexNode> getComplexExprs() {
    return complexExprs;
  }

  @Override
  public RexNode visitInputRef(RexInputRef inputRef) {
    return inputRef;
  }

  @Override
  public RexNode visitLocalRef(RexLocalRef localRef) {
    return localRef;
  }

  @Override
  public RexNode visitLiteral(RexLiteral literal) {
    return literal;
  }

  @Override
  public RexNode visitOver(RexOver over) {
    return over;
  }

  @Override
  public RexNode visitCorrelVariable(RexCorrelVariable correlVariable) {
    return correlVariable;
  }

  @Override
  public RexNode visitCall(RexCall call) {

    String functionName = call.getOperator().getName();

    List<RexNode> newOps = new ArrayList<>();
    for (RexNode operand : call.operands) {
      newOps.add(operand.accept(this));
    }
    if (funcReg.isFunctionComplexOutput(functionName) ) {
      RexBuilder builder = new RexBuilder(factory);
      RexNode ret = builder.makeInputRef( new RelDataTypeDrillImpl(new RelDataTypeHolder(), factory), lastUsedIndex);
      lastUsedIndex++;
      complexExprs.add(call.clone(new RelDataTypeDrillImpl(new RelDataTypeHolder(),factory), newOps));
      return ret;
    }
    return call.clone(call.getType(), newOps);
  }

  @Override
  public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
    return dynamicParam;
  }

  @Override
  public RexNode visitRangeRef(RexRangeRef rangeRef) {
    return rangeRef;
  }

  @Override
  public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
    return fieldAccess;
  }

}
