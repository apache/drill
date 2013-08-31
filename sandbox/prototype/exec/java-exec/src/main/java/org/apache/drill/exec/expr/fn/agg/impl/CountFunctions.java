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

public class CountFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CountFunctions.class);
  
  @FunctionTemplate(name = "count", scope = FunctionScope.POINT_AGGREGATE)
  public static class BigIntCount implements DrillAggFunc{

    @Param BigIntHolder in;
    @Workspace long count;
    @Output BigIntHolder out;
    
    public void setup(RecordBatch incoming) {
    }

    @Override
    public void add() {
      count++;
    }

    @Override
    public void output() {
      out.value = count;
    }

    @Override
    public void reset() {
      count = 0;
    }
    
  }
  
  @FunctionTemplate(name = "count", scope = FunctionScope.POINT_AGGREGATE)
  public static class IntCount implements DrillAggFunc{

    @Param IntHolder in;
    @Workspace long count;
    @Output BigIntHolder out;
    
    public void setup(RecordBatch incoming) {
    }

    @Override
    public void add() {
      count++;
    }

    @Override
    public void output() {
      out.value = count;
    }

    @Override
    public void reset() {
      count = 0;
    }
    
  }
}
