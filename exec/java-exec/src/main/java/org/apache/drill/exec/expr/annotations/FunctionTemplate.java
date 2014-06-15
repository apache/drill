/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.expr.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface FunctionTemplate {

  /**
   * Use this annotation if there is only one name for function
   * Note: If you use this annotation don't use {@link #names()}
   * @return
   */
  String name() default "";

  /**
   * Use this annotation if there are multiple names for function
   * Note: If you use this annotation don't use {@link #name()}
   * @return
   */
  String[] names() default {};

  FunctionScope scope();
  NullHandling nulls() default NullHandling.INTERNAL;
  boolean isBinaryCommutative() default false;
  boolean isRandom()  default false;
  
  public static enum NullHandling {
    INTERNAL, NULL_IF_NULL;
  }
  
  public static enum FunctionScope{
    SIMPLE,
    POINT_AGGREGATE,
    DECIMAL_AGGREGATE,
    HOLISTIC_AGGREGATE,
    RANGE_AGGREGATE,
    DECIMAL_MAX_SCALE,
    DECIMAL_SUM_SCALE,
    DECIMAL_CAST,
    DECIMAL_DIV_SCALE,
    DECIMAL_SET_SCALE,
    DECIMAL_ZERO_SCALE,
    SC_BOOLEAN_OPERATOR
  }
}
