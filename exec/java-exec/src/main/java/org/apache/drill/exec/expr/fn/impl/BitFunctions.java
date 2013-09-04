package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.record.RecordBatch;

public class BitFunctions {
  
  @FunctionTemplate(name = "or", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class BitOr implements DrillSimpleFunc {

    @Param BitHolder left;
    @Param BitHolder right;
    @Output BitHolder out;

    public void setup(RecordBatch incoming) {}

    public void eval() {
      out.value = left.value | right.value;
    }
  }  

  @FunctionTemplate(name = "and", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class BitAnd implements DrillSimpleFunc {

    @Param BitHolder left;
    @Param BitHolder right;
    @Output BitHolder out;

    public void setup(RecordBatch incoming) {}

    public void eval() {
      out.value = left.value & right.value;
    }
  }  
  
  
  @FunctionTemplate(name = "xor", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class IntXor implements DrillSimpleFunc {

    @Param IntHolder left;
    @Param IntHolder right;
    @Output IntHolder out;

    public void setup(RecordBatch incoming) {}

    public void eval() {
      out.value = left.value ^ right.value;
    }
  }  
  

}
