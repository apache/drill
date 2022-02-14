package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import java.math.BigInteger;

public class MathFunctionsVarcharInput {
  @FunctionTemplate(name = "rand", isRandom = true,
    scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class RandomWithSeed implements DrillSimpleFunc{
    @Param
    VarCharHolder seed;
    @Workspace
    java.util.Random rand;
    @Output  Float8Holder out;

    public void setup(){
      BigInteger seedBigInt = new BigInteger(String.valueOf(seed));
      rand = new java.util.Random(seedBigInt.intValue());
    }

    public void eval(){
      out.value = rand.nextDouble();
    }
  }

  @FunctionTemplate(name = "power", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class Power implements DrillSimpleFunc{

    @Param VarCharHolder a;
    @Param VarCharHolder b;
    @Output  Float8Holder out;

    public void setup(){

    }

    public void eval(){
      String aStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(a);
      double aDbl = Double.parseDouble(aStr);

      String bStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(b);
      double bDbl = Double.parseDouble(bStr);

      out.value = java.lang.Math.pow(aDbl, bDbl);

      System.out.println("out.value: " + out.value);
    }

  }

  @FunctionTemplate(name = "mod", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class Mod implements DrillSimpleFunc{

    @Param VarCharHolder a;
    @Param VarCharHolder b;
    @Output Float8Holder out;

    public void setup(){

    }

    public void eval(){
      String aStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(a);
      double aDbl = Double.parseDouble(aStr);

      String bStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(b);
      double bDbl = Double.parseDouble(bStr);

      out.value = aDbl % bDbl;

      System.out.println("out.value: " + out.value);
    }

  }

  @FunctionTemplate(name = "abs", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class Abs implements DrillSimpleFunc{

    @Param VarCharHolder a;
    @Output Float8Holder out;

    public void setup(){

    }

    public void eval(){
      String aStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(a);
      double aDbl = Double.parseDouble(aStr);

      out.value = Math.abs(aDbl);

      System.out.println("out.value: " + out.value);
    }
  }

  @FunctionTemplate(name = "cbrt", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class Cbrt implements DrillSimpleFunc{

    @Param VarCharHolder a;
    @Output Float8Holder out;

    public void setup(){

    }

    public void eval(){
      String aStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(a);
      double aDbl = Double.parseDouble(aStr);

      out.value = Math.cbrt(aDbl);

      System.out.println("out.value: " + out.value);
    }

  }

  @FunctionTemplate(names = {"ceil", "ceiling"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class Ceil implements DrillSimpleFunc{

    @Param VarCharHolder a;
    @Output Float8Holder out;

    public void setup(){

    }

    public void eval(){
      String aStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(a);
      double aDbl = Double.parseDouble(aStr);

      out.value = Math.ceil(aDbl);

      System.out.println("out.value: " + out.value);
    }

  }

  @FunctionTemplate(name = "degrees", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class Degrees implements DrillSimpleFunc{

    @Param VarCharHolder a;
    @Output Float8Holder out;

    public void setup(){

    }

    public void eval(){
      String aStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(a);
      double aDbl = Double.parseDouble(aStr);

      out.value = Math.toDegrees(aDbl);

      System.out.println("out.value: " + out.value);
    }

  }

  @FunctionTemplate(name = "exp", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class Exp implements DrillSimpleFunc{

    @Param VarCharHolder a;
    @Output Float8Holder out;

    public void setup(){

    }

    public void eval(){
      String aStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(a);
      double aDbl = Double.parseDouble(aStr);

      out.value = Math.exp(aDbl);

      System.out.println("out.value: " + out.value);
    }

  }

  @FunctionTemplate(name = "floor", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class Floor implements DrillSimpleFunc{

    @Param VarCharHolder a;
    @Output Float8Holder out;

    public void setup(){

    }

    public void eval(){
      String aStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(a);
      double aDbl = Double.parseDouble(aStr);

      out.value = Math.floor(aDbl);

      System.out.println("out.value: " + out.value);
    }

  }

  @FunctionTemplate(name = "log", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class Log implements DrillSimpleFunc{

    @Param VarCharHolder a;
    @Output Float8Holder out;

    public void setup(){

    }

    public void eval(){
      String aStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(a);
      double aDbl = Double.parseDouble(aStr);

      out.value = Math.log(aDbl);

      //System.out.println("out.value: " + out.value);
    }
  }

  @FunctionTemplate(name = "log", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class LogXY implements DrillSimpleFunc{

    @Param VarCharHolder base;
    @Param VarCharHolder val;
    @Output Float8Holder out;

    public void setup(){

    }

    public void eval(){
      String baseStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(base);
      double baseDbl = Double.parseDouble(baseStr);

      String valStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(val);
      double valDbl = Double.parseDouble(valStr);

      out.value = java.lang.Math.log(valDbl)/java.lang.Math.log(baseDbl);

      System.out.println("out.value: " + out.value);
    }
  }


@FunctionTemplate(name = "log10", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
public static class Log10 implements DrillSimpleFunc{

  @Param VarCharHolder a;
  @Output Float8Holder out;

  public void setup(){

  }

  public void eval(){
    String aStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(a);
    double aDbl = Double.parseDouble(aStr);

    out.value = Math.log10(aDbl);

    System.out.println("out.value: " + out.value);
  }
}

  @FunctionTemplate(name = "negative", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class Negative implements DrillSimpleFunc{

    @Param VarCharHolder a;
    @Output Float8Holder out;

    public void setup(){

    }

    public void eval(){
      String aStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(a);
      double aDbl = Double.parseDouble(aStr);

      out.value = -(aDbl);

      System.out.println("out.value: " + out.value);
    }
  }

  @FunctionTemplate(name = "lshift", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class Lshift implements DrillSimpleFunc{

    @Param VarCharHolder a;
    @Param VarCharHolder b;
    @Output Float8Holder out;

    public void setup(){

    }

    public void eval(){
      String aStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(a);
      int aInt = Integer.parseInt(aStr);

      String bStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(b);
      int bInt = Integer.parseInt(bStr);

      int result = (aInt << bInt);

      out.value = (double) result;

      System.out.println("out.value: " + out.value);
    }
  }

  @FunctionTemplate(name = "rshift", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class Rshift implements DrillSimpleFunc{

    @Param VarCharHolder a;
    @Param VarCharHolder b;
    @Output Float8Holder out;

    public void setup(){

    }

    public void eval(){
      String aStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(a);
      int aInt = Integer.parseInt(aStr);

      String bStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(b);
      int bInt = Integer.parseInt(bStr);

      int result = (aInt >> bInt);

      out.value = (double) result;

      System.out.println("out.value: " + out.value);
    }
  }

  @FunctionTemplate(name = "radians", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class Radians implements DrillSimpleFunc{

    @Param VarCharHolder a;
    @Output Float8Holder out;

    public void setup(){

    }

    public void eval(){
      String aStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(a);
      double aDbl = Double.parseDouble(aStr);

      out.value = Math.toRadians(aDbl);

      System.out.println("out.value: " + out.value);
    }
  }

  @FunctionTemplate(name = "round", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class Round implements DrillSimpleFunc{

    @Param VarCharHolder a;
    @Output Float8Holder out;

    public void setup(){

    }

    public void eval(){
      String aStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(a);
      double aDbl = Double.parseDouble(aStr);

      out.value = Math.round(aDbl);

      System.out.println("out.value: " + out.value);
    }
  }

  @FunctionTemplate(name = "round", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class Round2 implements DrillSimpleFunc {

    @Param VarCharHolder a;
    @Param VarCharHolder b;
    @Output Float8Holder out;

    public void setup() {
    }

    public void eval() {
      String aStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(a);
      double aDbl = Double.parseDouble(aStr);

      String bStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(b);
      int bInt = Integer.parseInt(bStr);

      java.math.BigDecimal temp = new java.math.BigDecimal(aDbl);
      out.value = temp.setScale(bInt, java.math.RoundingMode.HALF_UP).doubleValue();
    }
  }

  @FunctionTemplate(name = "sign", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class Sign implements DrillSimpleFunc{

    @Param VarCharHolder a;
    @Output IntHolder out;

    public void setup(){

    }

    public void eval(){
      String aStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(a);
      double aDbl = Double.parseDouble(aStr);

      out.value = (int) Math.signum(aDbl);

      System.out.println("out.value: " + out.value);
    }
  }

  @FunctionTemplate(name = "sqrt", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class Sqrt implements DrillSimpleFunc{

    @Param VarCharHolder a;
    @Output Float8Holder out;

    public void setup(){

    }

    public void eval(){
      String aStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(a);
      double aDbl = Double.parseDouble(aStr);

      out.value = Math.sqrt(aDbl);

      System.out.println("out.value: " + out.value);
    }
  }

  @FunctionTemplate(name = "trunc", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class Trunc implements DrillSimpleFunc{

    @Param VarCharHolder a;
    @Param VarCharHolder b;
    @Output Float8Holder out;

    public void setup(){

    }

    public void eval(){
      String aStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(a);
      double aDbl = Double.parseDouble(aStr);

      String bStr = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(b);
      int bInt = Integer.parseInt(bStr);

      if (Double.isInfinite(aDbl) || Double.isNaN(aDbl)) {
        out.value = Double.NaN;
      } else {
        java.math.BigDecimal temp = new java.math.BigDecimal(aDbl);
        out.value = temp.setScale(bInt, java.math.RoundingMode.DOWN).doubleValue();
      }

      System.out.println("out.value: " + out.value);
    }
  }
}
