package org.apache.drill.exec.expr.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;

/**
 * Marker annotation to determine which fields should be included as parameters for the function.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface Param {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Param.class);
}
