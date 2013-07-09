package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.LongHolder;
import org.apache.drill.exec.record.RecordBatch;

public class MathFunctions{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MathFunctions.class);
  
  private MathFunctions(){}
  
  @FunctionTemplate(name = "add", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Add1 implements DrillFunc{
    
    @Param IntHolder left;
    @Param IntHolder right;
    @Output IntHolder out;

    public void setup(RecordBatch b){}
    
    public void eval(){
      out.value = left.value + right.value;
    }

  }
  
  @FunctionTemplate(name = "add", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class LongAdd1 implements DrillFunc{
    
    @Param LongHolder left;
    @Param LongHolder right;
    @Output LongHolder out;

    public void setup(RecordBatch b){}
    
    public void eval(){
      out.value = left.value + right.value;
    }

  }
  
}
