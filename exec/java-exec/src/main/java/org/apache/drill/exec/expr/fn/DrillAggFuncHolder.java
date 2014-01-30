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

import java.util.List;
import java.util.Map;

import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.BlockType;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;

import com.google.common.base.Preconditions;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JVar;

class DrillAggFuncHolder extends DrillFuncHolder{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillAggFuncHolder.class);
  
  private final String setup;
  private final String reset;
  private final String add;
  private final String output;
  private final String cleanup;
  
  public DrillAggFuncHolder(FunctionScope scope, NullHandling nullHandling, boolean isBinaryCommutative,
      String functionName, ValueReference[] parameters, ValueReference returnValue, WorkspaceReference[] workspaceVars,
      Map<String, String> methods, List<String> imports) {
    super(scope, nullHandling, isBinaryCommutative, functionName, parameters, returnValue, workspaceVars, methods, imports);
    Preconditions.checkArgument(nullHandling == NullHandling.INTERNAL, "An aggregation function is required to do its own null handling.");
    setup = methods.get("setup");
    reset = methods.get("reset");
    add = methods.get("add");
    output = methods.get("output");
    cleanup = methods.get("cleanup");
    Preconditions.checkNotNull(add);
    Preconditions.checkNotNull(output);
    Preconditions.checkNotNull(reset);
  }

  public boolean isNested(){
    return true;
  }
  
  @Override
  public JVar[] renderStart(ClassGenerator<?> g, HoldingContainer[] inputVariables) {
    JVar[] workspaceJVars = declareWorkspaceVariables(g);
    generateBody(g, BlockType.SETUP, setup, workspaceJVars);
    return workspaceJVars;
  }


  @Override
  public void renderMiddle(ClassGenerator<?> g, HoldingContainer[] inputVariables, JVar[]  workspaceJVars) {
    addProtectedBlock(g, g.getBlock(BlockType.EVAL), add, inputVariables, workspaceJVars);
  }


  @Override
  public HoldingContainer renderEnd(ClassGenerator<?> g, HoldingContainer[] inputVariables, JVar[]  workspaceJVars) {
    HoldingContainer out = g.declare(returnValue.type, false);
    JBlock sub = new JBlock();
    g.getEvalBlock().add(sub);
    JVar internalOutput = sub.decl(JMod.FINAL, g.getHolderType(returnValue.type), returnValue.name, JExpr._new(g.getHolderType(returnValue.type)));
    addProtectedBlock(g, sub, output, null, workspaceJVars);
    sub.assign(out.getHolder(), internalOutput);

    generateBody(g, BlockType.RESET, reset, workspaceJVars);
    generateBody(g, BlockType.CLEANUP, cleanup, workspaceJVars);
    return out;
  }


}
