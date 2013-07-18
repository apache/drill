package org.apache.drill.exec.expr.fn;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.CodeGenerator.HoldingContainer;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JVar;

public class FunctionHolder {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionImplementationRegistry.class);
  
  private FunctionTemplate.FunctionScope scope;
  private FunctionTemplate.NullHandling nullHandling;
  private boolean isBinaryCommutative;
  private String functionName;
  private String evalBody;
  private String addBody;
  private String setupBody;
  private List<String> imports;
  private WorkspaceReference[] workspaceVars;
  private ValueReference[] parameters;
  private ValueReference returnValue;
  
  public FunctionHolder(FunctionScope scope, NullHandling nullHandling, boolean isBinaryCommutative, String functionName, ValueReference[] parameters, ValueReference returnValue, WorkspaceReference[] workspaceVars, Map<String, String> methods, List<String> imports) {
    super();
    this.scope = scope;
    this.nullHandling = nullHandling;
    this.workspaceVars = workspaceVars;
    this.isBinaryCommutative = isBinaryCommutative;
    this.functionName = functionName;
    this.setupBody = methods.get("setup");
    this.addBody = methods.get("add");
    this.evalBody = methods.get("eval");
    Preconditions.checkNotNull(evalBody);
    this.parameters = parameters;
    this.returnValue = returnValue;
    this.imports = imports;
  }
  
  public List<String> getImports() {
    return imports;
  }

  private void generateSetupBody(CodeGenerator<?> g){
    if(!Strings.isNullOrEmpty(setupBody)){
      JBlock sub = new JBlock(true, true);
      addProtectedBlock(g, sub, setupBody, null);
      g.getSetupBlock().directStatement(String.format("/** start setup for function %s **/", functionName));
      g.getSetupBlock().add(sub);
      g.getSetupBlock().directStatement(String.format("/** end setup for function %s **/", functionName));
    }
  }
  
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

  public HoldingContainer renderFunction(CodeGenerator<?> g, HoldingContainer[] inputVariables){
    generateSetupBody(g);
    return generateEvalBody(g, inputVariables);
  }
  
  private HoldingContainer generateEvalBody(CodeGenerator<?> g, HoldingContainer[] inputVariables){
    
    //g.getBlock().directStatement(String.format("//---- start of eval portion of %s function. ----//", functionName));
    
    JBlock sub = new JBlock(true, true);
    
    HoldingContainer out = null;

    // add outside null handling if it is defined.
    if(nullHandling == NullHandling.NULL_IF_NULL){
      JExpression e = null;
      for(HoldingContainer v : inputVariables){
        if(v.isOptional()){
          if(e == null){
            e = v.getIsSet();
          }else{
            e = e.mul(v.getIsSet());
          }
        }
      }
      
      if(e != null){
        // if at least one expression must be checked, set up the conditional.
        returnValue.type = returnValue.type.toBuilder().setMode(DataMode.OPTIONAL).build();
        out = g.declare(returnValue.type, false);
        e = e.eq(JExpr.lit(0));
        JConditional jc = sub._if(e);
        jc._then().assign(out.getIsSet(), JExpr.lit(0));
        sub = jc._else();
      }
    }
    
    if(out == null) out = g.declare(returnValue.type);
    
    // add the subblock after the out declaration.
    g.getBlock().add(sub);
    
    
    JVar internalOutput = sub.decl(JMod.FINAL, g.getHolderType(returnValue.type), returnValue.name, JExpr._new(g.getHolderType(returnValue.type)));
    addProtectedBlock(g, sub, evalBody, inputVariables);
    sub.assign(out.getHolder(), internalOutput);

    return out;
  }
  
  
  
  public boolean matches(FunctionCall call){
    if(!softCompare(call.getMajorType(), returnValue.type)) return false;
    if(call.args.size() != parameters.length) return false;
    for(int i =0; i < parameters.length; i++){
      ValueReference param = parameters[i];
      LogicalExpression arg = call.args.get(i);
      if(!softCompare(param.type, arg.getMajorType())) return false;
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
  @Override
  public String toString() {
    final int maxLen = 10;
    return "FunctionHolder [scope=" + scope + ", isBinaryCommutative=" + isBinaryCommutative + ", functionName="
        + functionName + ", evalBody=" + evalBody + ", addBody=" + addBody + ", setupBody=" + setupBody
        + ", parameters="
        + (parameters != null ? Arrays.asList(parameters).subList(0, Math.min(parameters.length, maxLen)) : null)
        + ", returnValue=" + returnValue + "]";
  }
  
  
}
