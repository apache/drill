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

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.BlockType;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.record.NullExpression;
import org.apache.drill.exec.resolver.ResolverTypePrecedence;
import org.apache.drill.exec.resolver.TypeCastRules;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JVar;

public abstract class DrillFuncHolder {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionImplementationRegistry.class);
  
  protected final FunctionTemplate.FunctionScope scope;
  protected final FunctionTemplate.NullHandling nullHandling;
  protected final boolean isBinaryCommutative;
  protected final String functionName;
  protected final ImmutableList<String> imports;
  protected final WorkspaceReference[] workspaceVars;
  protected final ValueReference[] parameters;
  protected final ValueReference returnValue;
  protected final ImmutableMap<String, String> methodMap; 
  
  public DrillFuncHolder(FunctionScope scope, NullHandling nullHandling, boolean isBinaryCommutative, String functionName, ValueReference[] parameters, ValueReference returnValue, WorkspaceReference[] workspaceVars, Map<String, String> methods, List<String> imports) {
    super();
    this.scope = scope;
    this.nullHandling = nullHandling;
    this.workspaceVars = workspaceVars;
    this.isBinaryCommutative = isBinaryCommutative;
    this.functionName = functionName;
    this.methodMap = ImmutableMap.copyOf(methods);
    this.parameters = parameters;
    this.returnValue = returnValue;
    this.imports = ImmutableList.copyOf(imports);
    
  }
  
  public List<String> getImports() {
    return imports;
  }

  public JVar[] renderStart(ClassGenerator<?> g, HoldingContainer[] inputVariables){
    return declareWorkspaceVariables(g);
  };
  
  public void renderMiddle(ClassGenerator<?> g, HoldingContainer[] inputVariables, JVar[]  workspaceJVars){};  
  public abstract HoldingContainer renderEnd(ClassGenerator<?> g, HoldingContainer[] inputVariables, JVar[]  workspaceJVars);
  public abstract boolean isNested();
  
  protected JVar[] declareWorkspaceVariables(ClassGenerator<?> g){
    JVar[] workspaceJVars = new JVar[workspaceVars.length];
    for(int i =0 ; i < workspaceVars.length; i++){
      workspaceJVars[i] = g.declareClassField("work", g.getModel()._ref(workspaceVars[i].type));
    }
    return workspaceJVars;
  }

  protected void generateBody(ClassGenerator<?> g, BlockType bt, String body, JVar[] workspaceJVars){
    if(!Strings.isNullOrEmpty(body) && !body.trim().isEmpty()){
      JBlock sub = new JBlock(true, true);
      addProtectedBlock(g, sub, body, null, workspaceJVars);
      g.getBlock(bt).directStatement(String.format("/** start %s for function %s **/ ", bt.name(), functionName));
      g.getBlock(bt).add(sub);
      g.getBlock(bt).directStatement(String.format("/** end %s for function %s **/ ", bt.name(), functionName));
    }
  }

  protected void addProtectedBlock(ClassGenerator<?> g, JBlock sub, String body, HoldingContainer[] inputVariables, JVar[] workspaceJVars){

    if(inputVariables != null){
      for(int i =0; i < inputVariables.length; i++){
        ValueReference parameter = parameters[i];
        HoldingContainer inputVariable = inputVariables[i];
        sub.decl(inputVariable.getHolder().type(), parameter.name, inputVariable.getHolder());  
      }
    }

    JVar[] internalVars = new JVar[workspaceJVars.length];
    for(int i =0; i < workspaceJVars.length; i++){
      internalVars[i] = sub.decl(g.getModel()._ref(workspaceVars[i].type),  workspaceVars[i].name, workspaceJVars[i]);
    }
    
    Preconditions.checkNotNull(body);
    sub.directStatement(body);
    
    // reassign workspace variables back to global space.
    for(int i =0; i < workspaceJVars.length; i++){
      sub.assign(workspaceJVars[i], internalVars[i]);
    }
  }

 
  
  
  public boolean matches(FunctionCall call){
    
    if(!softCompare(call.getMajorType(), returnValue.type)){
//      logger.debug(String.format("Call [%s] didn't match as return type [%s] was different than expected [%s]. ", call.getDefinition().getName(), returnValue.type, call.getMajorType()));
      return false;
    }
    
    if(call.args.size() != parameters.length){
//      logger.debug(String.format("Call [%s] didn't match as the number of arguments provided [%d] were different than expected [%d]. ", call.getDefinition().getName(), parameters.length, call.args.size()));
      return false;
    }
    for(int i =0; i < parameters.length; i++){
      ValueReference param = parameters[i];
      LogicalExpression arg = call.args.get(i);
      if(!softCompare(param.type, arg.getMajorType())){
//        logger.debug(String.format("Call [%s] didn't match as the argument [%s] didn't match the expected type [%s]. ", call.getDefinition().getName(), arg.getMajorType(), param.type));
        return false;
      }
    }
    
    return true;
  }
  	    
  public MajorType getParmMajorType(int i) {
    return this.parameters[i].type;
  }
  
  public int getParmSize(){
    return this.parameters.length;
  }
  
  public NullHandling getNullHandling() {
    return this.nullHandling ;
  }
  
  private boolean softCompare(MajorType a, MajorType b){
    return Types.softEquals(a, b, nullHandling == NullHandling.NULL_IF_NULL);
  }
  
  public String getFunctionName() {
    return functionName;
  }

  public static class ValueReference{
    MajorType type;
    String name;
    public ValueReference(MajorType type, String name) {
      super();
      Preconditions.checkNotNull(type);
      Preconditions.checkNotNull(name);
      this.type = type;
      this.name = name;
    }
    @Override
    public String toString() {
      return "ValueReference [type=" + type + ", name=" + name + "]";
    }
    
  }

  
  public static class WorkspaceReference{
    Class<?> type;
    String name;


    public WorkspaceReference(Class<?> type, String name) {
      super();
      Preconditions.checkNotNull(type);
      Preconditions.checkNotNull(name);
      this.type = type;
      this.name = name;
    }
    
  }

  
  
}
