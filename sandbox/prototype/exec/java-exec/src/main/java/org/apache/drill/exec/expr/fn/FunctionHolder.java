package org.apache.drill.exec.expr.fn;

import java.util.List;
import java.util.Map;

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.CodeGenerator.HoldingContainer;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JVar;

public abstract class FunctionHolder {

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
  
  public FunctionHolder(FunctionScope scope, NullHandling nullHandling, boolean isBinaryCommutative, String functionName, ValueReference[] parameters, ValueReference returnValue, WorkspaceReference[] workspaceVars, Map<String, String> methods, List<String> imports) {
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

  public abstract HoldingContainer renderFunction(CodeGenerator<?> g, HoldingContainer[] inputVariables);
  
  public void addProtectedBlock(CodeGenerator<?> g, JBlock sub, String body, HoldingContainer[] inputVariables){
    
    // create sub block with defined workspace variables.
    JVar[] workspaceJVars = new JVar[workspaceVars.length];
    for(int i =0 ; i < workspaceVars.length; i++){
      workspaceJVars[i] = g.declareClassField("work", g.getModel()._ref(workspaceVars[i].type));
    }

    if(inputVariables != null){
      for(int i =0; i < inputVariables.length; i++){
        ValueReference parameter = parameters[i];
        HoldingContainer inputVariable = inputVariables[i];
        sub.decl(JMod.FINAL, inputVariable.getHolder().type(), parameter.name, inputVariable.getHolder());  
      }
    }

    JVar[] internalVars = new JVar[workspaceJVars.length];
    for(int i =0; i < workspaceJVars.length; i++){
      internalVars[i] = sub.decl(JMod.FINAL, g.getModel()._ref(workspaceVars[i].type),  workspaceVars[i].name, workspaceJVars[i]);
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
