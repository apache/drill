package org.apache.drill.exec.expr.fn.agg.impl;

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.record.RecordBatch;

public class SumFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SumFunctions.class);
  
  @FunctionTemplate(name = "sum", scope = FunctionScope.POINT_AGGREGATE)
  public static class BigIntSum implements DrillAggFunc{

    @Param BigIntHolder in;
    @Workspace long sum;
    @Output BigIntHolder out;
    
    public void setup(RecordBatch incoming) {
    }

    @Override
    public void add() {
      sum += in.value;
    }

    @Override
    public void output() {
      out.value = sum;
    }

    @Override
    public void reset() {
      sum = 0;
    }
    
  }
  
  @FunctionTemplate(name = "sum", scope = FunctionScope.POINT_AGGREGATE)
  public static class IntSum implements DrillAggFunc{

    @Param IntHolder in;
    @Workspace int sum;
    @Output IntHolder out;
    
    public void setup(RecordBatch incoming) {
    }

    @Override
    public void add() {
      sum += in.value;
    }

    @Override
    public void output() {
      out.value = sum;
    }

    @Override
    public void reset() {
      sum = 0;
    }
    
  }
}
