package org.apache.drill.exec.expr.fn;

import java.util.List;
import java.util.Map;

import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.CodeGenerator.HoldingContainer;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;

public class DrillAggFuncHolder extends DrillFuncHolder{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillAggFuncHolder.class);
  
  
  public DrillAggFuncHolder(FunctionScope scope, NullHandling nullHandling, boolean isBinaryCommutative,
      String functionName, ValueReference[] parameters, ValueReference returnValue, WorkspaceReference[] workspaceVars,
      Map<String, String> methods, List<String> imports) {
    super(scope, nullHandling, isBinaryCommutative, functionName, parameters, returnValue, workspaceVars, methods, imports);
    
  }

  public HoldingContainer renderFunction(CodeGenerator<?> g, HoldingContainer[] inputVariables){
    g.getMappingSet().enterChild();
    renderInside(g, inputVariables);
    g.getMappingSet().exitChild();
    return renderOutside(g);
  }
  
  private void renderInside(CodeGenerator<?> g, HoldingContainer[] inputVariables){
    
  }
  
  private HoldingContainer renderOutside(CodeGenerator<?> g){
    return null;
  }
}
