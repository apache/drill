package org.apache.drill.exec.expr.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Describes the field will provide output from the given function.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface Output {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Output.class);

  

}
