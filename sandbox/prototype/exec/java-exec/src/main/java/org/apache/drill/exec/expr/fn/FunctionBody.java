package org.apache.drill.exec.expr.fn;

public class FunctionBody {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionBody.class);
  
  
  public static enum BodyType{
    SETUP, EVAL_INNER, EVAL_OUTER, 
  }
  
  
}
