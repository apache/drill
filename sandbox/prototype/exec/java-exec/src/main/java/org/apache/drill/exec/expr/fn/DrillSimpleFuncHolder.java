package org.apache.drill.exec.expr.fn;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.CodeGenerator.BlockType;
import org.apache.drill.exec.expr.CodeGenerator.HoldingContainer;
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

class DrillSimpleFuncHolder extends DrillFuncHolder{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSimpleFuncHolder.class);
  
  private final String setupBody;
  private final String evalBody;
  private final String resetBody;
  private final String cleanupBody;
  
  
  public DrillSimpleFuncHolder(FunctionScope scope, NullHandling nullHandling, boolean isBinaryCommutative,
      String functionName, ValueReference[] parameters, ValueReference returnValue, WorkspaceReference[] workspaceVars,
      Map<String, String> methods, List<String> imports) {
    super(scope, nullHandling, isBinaryCommutative, functionName, parameters, returnValue, workspaceVars, methods, imports);
    setupBody = methods.get("setup");
    evalBody = methods.get("eval");
    resetBody = methods.get("reset");
    cleanupBody = methods.get("cleanup");
    Preconditions.checkNotNull(evalBody);
    
  }

  public boolean isNested(){
    return false;
  }
  
  public HoldingContainer renderEnd(CodeGenerator<?> g, HoldingContainer[] inputVariables, JVar[]  workspaceJVars){
    generateBody(g, BlockType.SETUP, setupBody, workspaceJVars);
    HoldingContainer c = generateEvalBody(g, inputVariables, evalBody, workspaceJVars);
    generateBody(g, BlockType.RESET, resetBody, workspaceJVars);
    generateBody(g, BlockType.CLEANUP, cleanupBody, workspaceJVars);
    return c;
  }
  
 protected HoldingContainer generateEvalBody(CodeGenerator<?> g, HoldingContainer[] inputVariables, String body, JVar[] workspaceJVars){
    
    //g.getBlock().directStatement(String.format("//---- start of eval portion of %s function. ----//", functionName));
    
    JBlock sub = new JBlock(true, true);
    JBlock topSub = sub;
    HoldingContainer out = null;
    MajorType returnValueType = returnValue.type;

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
        returnValueType = returnValue.type.toBuilder().setMode(DataMode.OPTIONAL).build();
        out = g.declare(returnValueType);
        e = e.eq(JExpr.lit(0));
        JConditional jc = sub._if(e);
        jc._then().assign(out.getIsSet(), JExpr.lit(0));
        sub = jc._else();
      }
    }
    
    if(out == null) out = g.declare(returnValueType);
    
    // add the subblock after the out declaration.
    g.getEvalBlock().add(topSub);
    
    
    JVar internalOutput = sub.decl(JMod.FINAL, g.getHolderType(returnValueType), returnValue.name, JExpr._new(g.getHolderType(returnValueType)));
    addProtectedBlock(g, sub, body, inputVariables, workspaceJVars);
    if (sub != topSub) sub.assign(internalOutput.ref("isSet"),JExpr.lit(1));// Assign null if NULL_IF_NULL mode
    sub.assign(out.getHolder(), internalOutput);
    if (sub != topSub) sub.assign(internalOutput.ref("isSet"),JExpr.lit(1));// Assign null if NULL_IF_NULL mode
    return out;
  }

@Override
public String toString() {
  final int maxLen = 10;
  return "DrillSimpleFuncHolder [, functionName=" + functionName + ", nullHandling=" + nullHandling + "parameters="
      + (parameters != null ? Arrays.asList(parameters).subList(0, Math.min(parameters.length, maxLen)) : null) + "]";
}
  
}
