package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.common.expression.ArgumentValidators;
import org.apache.drill.common.expression.CallProvider;
import org.apache.drill.common.expression.FunctionDefinition;
import org.apache.drill.common.expression.OutputTypeDeterminer;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.DrillFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.BigIntHolder;
import org.apache.drill.exec.vector.IntHolder;

public class ComparatorFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ComparatorFunctions.class);
  
  @FunctionTemplate(name = "compare_to", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class IntComparator implements DrillFunc {

      @Param IntHolder left;
      @Param IntHolder right;
      @Output IntHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {
        out.value = left.value < right.value ? -1 : ((left.value == right.value)? 0 : 1);
      }
  }
  
  @FunctionTemplate(name = "compare_to", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Long implements DrillFunc {

      @Param BigIntHolder left;
      @Param BigIntHolder right;
      @Output IntHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {
        out.value = left.value < right.value ? -1 : ((left.value == right.value)? 0 : 1);
      }
  }
  public static final FunctionDefinition COMPARE_TO = FunctionDefinition.simple("compare_to", new ArgumentValidators.AllowedTypeList(2, Types.required(MinorType.INT)), new OutputTypeDeterminer.FixedType(Types.required(MinorType.INT)));
  public static class Provider implements CallProvider{

    @Override
    public FunctionDefinition[] getFunctionDefintions() {
      return new FunctionDefinition[]{
          COMPARE_TO
      };
    }
    
  }
}
