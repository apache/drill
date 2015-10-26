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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.BlockType;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionCostCategory;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.record.TypedFieldId;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JForLoop;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;

class DrillAggFuncHolder extends DrillFuncHolder {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillAggFuncHolder.class);

  private String setup() {
    return meth("setup");
  }
  private String reset() {
    return meth("reset", false);
  }
  private String add() {
    return meth("add");
  }
  private String output() {
    return meth("output");
  }
  private String cleanup() {
    return meth("cleanup", false);
  }

  public DrillAggFuncHolder(
      FunctionAttributes attributes,
      FunctionInitializer initializer) {
    super(attributes, initializer);
    checkArgument(attributes.getNullHandling() == NullHandling.INTERNAL, "An aggregation function is required to do its own null handling.");
  }

  @Override
  public boolean isNested(){
    return true;
  }

  @Override
  public boolean isAggregating() {
    return true;
  }

  @Override
  public JVar[] renderStart(ClassGenerator<?> g, HoldingContainer[] inputVariables) {
    if (!g.getMappingSet().isHashAggMapping()) {  //Declare workspace vars for non-hash-aggregation.
        JVar[] workspaceJVars = declareWorkspaceVariables(g);
        generateBody(g, BlockType.SETUP, setup(), null, workspaceJVars, true);
        return workspaceJVars;
      } else {  //Declare workspace vars and workspace vectors for hash aggregation.

        JVar[] workspaceJVars = declareWorkspaceVectors(g);

        JBlock setupBlock = g.getSetupBlock();

        //Loop through all workspace vectors, to get the minimum of size of all workspace vectors.
        JVar sizeVar = setupBlock.decl(g.getModel().INT, "vectorSize", JExpr.lit(Integer.MAX_VALUE));
        JClass mathClass = g.getModel().ref(Math.class);
        for (int id = 0; id<workspaceVars.length; id ++) {
          if (!workspaceVars[id].isInject()) {
            setupBlock.assign(sizeVar,mathClass.staticInvoke("min").arg(sizeVar).arg(g.getWorkspaceVectors().get(workspaceVars[id]).invoke("getValueCapacity")));
          }
        }

        for(int i =0 ; i < workspaceVars.length; i++) {
          if (!workspaceVars[i].isInject()) {
            setupBlock.assign(workspaceJVars[i], JExpr._new(g.getHolderType(workspaceVars[i].majorType)));
          }
        }

        //Use for loop to initialize entries in the workspace vectors.
        JForLoop forLoop = setupBlock._for();
        JVar ivar = forLoop.init(g.getModel().INT, "drill_internal_i", JExpr.lit(0));
        forLoop.test(ivar.lt(sizeVar));
        forLoop.update(ivar.assignPlus(JExpr.lit(1)));

        JBlock subBlock = generateInitWorkspaceBlockHA(g, BlockType.SETUP, setup(), workspaceJVars, ivar);
        forLoop.body().add(subBlock);
        return workspaceJVars;
      }


  }


  @Override
  public void renderMiddle(ClassGenerator<?> g, HoldingContainer[] inputVariables, JVar[]  workspaceJVars) {
    addProtectedBlock(g, g.getBlock(BlockType.EVAL), add(), inputVariables, workspaceJVars, false);
  }


  @Override
  public HoldingContainer renderEnd(ClassGenerator<?> g, HoldingContainer[] inputVariables, JVar[]  workspaceJVars) {
    HoldingContainer out = g.declare(returnValue.type, false);
    JBlock sub = new JBlock();
    g.getEvalBlock().add(sub);
    JVar internalOutput = sub.decl(JMod.FINAL, g.getHolderType(returnValue.type), returnValue.name, JExpr._new(g.getHolderType(returnValue.type)));
    addProtectedBlock(g, sub, output(), null, workspaceJVars, false);
    sub.assign(out.getHolder(), internalOutput);
        //hash aggregate uses workspace vectors. Initialization is done in "setup" and does not require "reset" block.
        if (!g.getMappingSet().isHashAggMapping()) {
          generateBody(g, BlockType.RESET, reset(), null, workspaceJVars, false);
        }
       generateBody(g, BlockType.CLEANUP, cleanup(), null, workspaceJVars, false);

    return out;
  }


  private JVar[] declareWorkspaceVectors(ClassGenerator<?> g) {
    JVar[] workspaceJVars = new JVar[workspaceVars.length];

    for(int i =0 ; i < workspaceVars.length; i++){
      if (workspaceVars[i].isInject() == true) {
        workspaceJVars[i] = g.declareClassField("work", g.getModel()._ref(workspaceVars[i].type));
        g.getBlock(BlockType.SETUP).assign(workspaceJVars[i], g.getMappingSet().getIncoming().invoke("getContext").invoke("getManagedBuffer"));
      } else {
        Preconditions.checkState(Types.isFixedWidthType(workspaceVars[i].majorType), String.format("Workspace variable '%s' in aggregation function '%s' is not allowed to have variable length type.", workspaceVars[i].name, registeredNames[0]));
        Preconditions.checkState(workspaceVars[i].majorType.getMode()==DataMode.REQUIRED, String.format("Workspace variable '%s' in aggregation function '%s' is not allowed to have null or repeated type.", workspaceVars[i].name, registeredNames[0]));

        //workspaceJVars[i] = g.declareClassField("work", g.getHolderType(workspaceVars[i].majorType), JExpr._new(g.getHolderType(workspaceVars[i].majorType)));
        workspaceJVars[i] = g.declareClassField("work", g.getHolderType(workspaceVars[i].majorType));

        //Declare a workspace vector for the workspace var.
        TypedFieldId typedFieldId = new TypedFieldId(workspaceVars[i].majorType, g.getWorkspaceTypes().size());
        JVar vv  = g.declareVectorValueSetupAndMember(g.getMappingSet().getWorkspace(), typedFieldId);

        g.getWorkspaceTypes().add(typedFieldId);
        g.getWorkspaceVectors().put(workspaceVars[i], vv);
      }
    }
    return workspaceJVars;
  }

  private JBlock generateInitWorkspaceBlockHA(ClassGenerator<?> g, BlockType bt, String body, JVar[] workspaceJVars, JExpression wsIndexVariable){
    JBlock initBlock = new JBlock(true, true);
    if(!Strings.isNullOrEmpty(body) && !body.trim().isEmpty()){
      JBlock sub = new JBlock(true, true);
      addProtectedBlockHA(g, sub, body, null, workspaceJVars, wsIndexVariable);
      initBlock.directStatement(String.format("/** start %s for function %s **/ ", bt.name(), registeredNames[0]));
      initBlock.add(sub);
      initBlock.directStatement(String.format("/** end %s for function %s **/ ", bt.name(), registeredNames[0]));
    }
    return initBlock;
  }

  @Override
  protected void addProtectedBlock(ClassGenerator<?> g, JBlock sub, String body, HoldingContainer[] inputVariables, JVar[] workspaceJVars, boolean decConstantInputOnly){
    if (!g.getMappingSet().isHashAggMapping()) {
      super.addProtectedBlock(g, sub, body, inputVariables, workspaceJVars, decConstantInputOnly);
    } else {
      JExpression indexVariable = g.getMappingSet().getWorkspaceIndex();
      addProtectedBlockHA(g, sub, body, inputVariables, workspaceJVars, indexVariable);
    }
  }

  /*
   * This is customized version of "addProtectedBlock" for hash aggregation. It take one additional parameter "wsIndexVariable".
   */
  private void addProtectedBlockHA(ClassGenerator<?> g, JBlock sub, String body, HoldingContainer[] inputVariables, JVar[] workspaceJVars, JExpression wsIndexVariable){
    if (inputVariables != null){
      for(int i =0; i < inputVariables.length; i++){
        ValueReference parameter = parameters[i];
        HoldingContainer inputVariable = inputVariables[i];
        sub.decl(inputVariable.getHolder().type(), parameter.name, inputVariable.getHolder());
      }
    }

    JVar[] internalVars = new JVar[workspaceJVars.length];
    for(int i =0; i < workspaceJVars.length; i++){

      if (workspaceVars[i].isInject()) {
        internalVars[i] = sub.decl(g.getModel()._ref(workspaceVars[i].type), workspaceVars[i].name, workspaceJVars[i]);
        continue;
      }
      //sub.assign(workspaceJVars[i], JExpr._new(g.getHolderType(workspaceVars[i].majorType)));
      //Access workspaceVar through workspace vector.
      JInvocation getValueAccessor = g.getWorkspaceVectors().get(workspaceVars[i]).invoke("getAccessor").invoke("get");
      if (Types.usesHolderForGet(workspaceVars[i].majorType)) {
        sub.add(getValueAccessor.arg(wsIndexVariable).arg(workspaceJVars[i]));
      } else {
        sub.assign(workspaceJVars[i].ref("value"), getValueAccessor.arg(wsIndexVariable));
      }
      internalVars[i] = sub.decl(g.getHolderType(workspaceVars[i].majorType),  workspaceVars[i].name, workspaceJVars[i]);
    }

    Preconditions.checkNotNull(body);
    sub.directStatement(body);

    // reassign workspace variables back.
    for(int i =0; i < workspaceJVars.length; i++){
      sub.assign(workspaceJVars[i], internalVars[i]);

      // Injected buffers are not stored as vectors skip storing them in vectors
      if (workspaceVars[i].isInject()) {
        continue;
      }
      //Change workspaceVar through workspace vector.
      JInvocation setMeth;
      MajorType type = workspaceVars[i].majorType;
      if (Types.usesHolderForGet(type)) {
          setMeth = g.getWorkspaceVectors().get(workspaceVars[i]).invoke("getMutator").invoke("setSafe").arg(wsIndexVariable).arg(workspaceJVars[i]);
      }else{
        if (!Types.isFixedWidthType(type) || Types.isRepeated(type)) {
          setMeth = g.getWorkspaceVectors().get(workspaceVars[i]).invoke("getMutator").invoke("setSafe").arg(wsIndexVariable).arg(workspaceJVars[i].ref("value"));
        } else {
          setMeth = g.getWorkspaceVectors().get(workspaceVars[i]).invoke("getMutator").invoke("set").arg(wsIndexVariable).arg(workspaceJVars[i].ref("value"));
        }
      }

      sub.add(setMeth);

      JClass drillRunTimeException = g.getModel().ref(DrillRuntimeException.class);
    }

  }

}
