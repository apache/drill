package org.apache.drill.exec.udfs;

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;

public class BenfordDistributionFunction {

  @FunctionTemplate( name = "bedford",
    scope = FunctionScope.POINT_AGGREGATE,
    nulls = NullHandling.INTERNAL)
  public static class BedfordDistribution implements DrillAggFunc {

  }

}
