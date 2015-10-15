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
package org.apache.drill.exec.expr.fn;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.BlockType;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;

import com.sun.codemodel.JBlock;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JVar;

public class DrillSimpleFuncHolder extends DrillFuncHolder {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSimpleFuncHolder.class);

  private final String drillFuncClass;

  public DrillSimpleFuncHolder(FunctionAttributes functionAttributes, FunctionInitializer initializer) {
    super(functionAttributes, initializer);
    drillFuncClass = checkNotNull(initializer.getClassName());
  }

  private String setupBody() {
    return meth("setup", false);
  }
  private String evalBody() {
    return meth("eval");
  }
  private String resetBody() {
    return meth("reset", false);
  }
  private String cleanupBody() {
    return meth("cleanup", false);
  }

  @Override
  public boolean isNested() {
    return false;
  }

  public DrillSimpleFunc createInterpreter() throws Exception {
    return (DrillSimpleFunc)Class.forName(drillFuncClass).newInstance();
  }

  @Override
  public HoldingContainer renderEnd(ClassGenerator<?> g, HoldingContainer[] inputVariables, JVar[]  workspaceJVars){
    //If the function's annotation specifies a parameter has to be constant expression, but the HoldingContainer
    //for the argument is not, then raise exception.
    for (int i =0; i < inputVariables.length; i++) {
      if (parameters[i].isConstant && !inputVariables[i].isConstant()) {
        throw new DrillRuntimeException(String.format("The argument '%s' of Function '%s' has to be constant!", parameters[i].name, this.getRegisteredNames()[0]));
      }
    }
    generateBody(g, BlockType.SETUP, setupBody(), inputVariables, workspaceJVars, true);
    HoldingContainer c = generateEvalBody(g, inputVariables, evalBody(), workspaceJVars);
    generateBody(g, BlockType.RESET, resetBody(), null, workspaceJVars, false);
    generateBody(g, BlockType.CLEANUP, cleanupBody(), null, workspaceJVars, false);
    return c;
  }

  protected HoldingContainer generateEvalBody(ClassGenerator<?> g, HoldingContainer[] inputVariables, String body, JVar[] workspaceJVars) {

    g.getEvalBlock().directStatement(String.format("//---- start of eval portion of %s function. ----//", registeredNames[0]));

    JBlock sub = new JBlock(true, true);
    JBlock topSub = sub;
    HoldingContainer out = null;
    MajorType returnValueType = returnValue.type;

    // add outside null handling if it is defined.
    if (nullHandling == NullHandling.NULL_IF_NULL) {
      JExpression e = null;
      for (HoldingContainer v : inputVariables) {
        if (v.isOptional()) {
          if (e == null) {
            e = v.getIsSet();
          } else {
            e = e.mul(v.getIsSet());
          }
        }
      }

      if (e != null) {
        // if at least one expression must be checked, set up the conditional.
        returnValueType = returnValue.type.toBuilder().setMode(DataMode.OPTIONAL).build();
        out = g.declare(returnValueType);
        e = e.eq(JExpr.lit(0));
        JConditional jc = sub._if(e);
        jc._then().assign(out.getIsSet(), JExpr.lit(0));
        sub = jc._else();
      }
    }

    if (out == null) {
      out = g.declare(returnValueType);
    }

    // add the subblock after the out declaration.
    g.getEvalBlock().add(topSub);


    JVar internalOutput = sub.decl(JMod.FINAL, g.getHolderType(returnValueType), returnValue.name, JExpr._new(g.getHolderType(returnValueType)));
    addProtectedBlock(g, sub, body, inputVariables, workspaceJVars, false);
    if (sub != topSub) {
      sub.assign(internalOutput.ref("isSet"),JExpr.lit(1));// Assign null if NULL_IF_NULL mode
    }
    sub.assign(out.getHolder(), internalOutput);
    if (sub != topSub) {
      sub.assign(internalOutput.ref("isSet"),JExpr.lit(1));// Assign null if NULL_IF_NULL mode
    }

    g.getEvalBlock().directStatement(String.format("//---- end of eval portion of %s function. ----//", registeredNames[0]));

    return out;
  }
}
