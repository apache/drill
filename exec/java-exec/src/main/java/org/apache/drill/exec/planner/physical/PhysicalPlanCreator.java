package org.apache.drill.exec.planner.physical;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rex.RexNode;

public class PhysicalPlanCreator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PhysicalPlanCreator.class);
  
  public LogicalExpression getExpression(RexNode e, RelNode input){
    return null;
  }
  
  public DrillParseContext getContext(){
    return null;
  }
  
  
}
