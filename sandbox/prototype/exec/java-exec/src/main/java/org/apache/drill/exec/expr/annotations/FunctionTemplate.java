package org.apache.drill.exec.expr.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface FunctionTemplate {
  
  String name();
  FunctionScope scope();
  NullHandling nulls() default NullHandling.INTERNAL;
  boolean isBinaryCommutative() default false;
  
  public static enum NullHandling {
    INTERNAL, NULL_IF_NULL;
  }
  
  public static enum FunctionScope{
    SIMPLE, AGGREGATE, RUNNING;
  }
}
