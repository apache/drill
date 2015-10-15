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
   * Name of function (when only one.)
   * Use this annotation element if there is only one name for the function.
   * Note: If you use this annotation don't use {@link #names()}.
   * <p>
   *   TODO:  Refer to wherever list of possible or at least known names is,
   *   to resolve the current issue of spaces vs. underlines in names (e.g., we
   *   have both "less_than" and "less than".
   * </p>
   * @return
   */
  String name() default "";

  /**
   * Names of function (when multiple).
   * Use this annotation element if there are multiple names for the function.
   * Note: If you use this annotation don't use {@link #name()}.
   * <p>
   *   TODO:  Refer to wherever list of possible or at least known names is,
   *   to resolve the current issue of spaces vs. underlines in names (e.g., we
   *   have both "less_than" and "less than".
   * </p>
   * @return
   */
  String[] names() default {};

  FunctionScope scope();
  NullHandling nulls() default NullHandling.INTERNAL;
  boolean isBinaryCommutative() default false;
  boolean isRandom()  default false;
  String desc() default "";
  FunctionCostCategory costCategory() default FunctionCostCategory.SIMPLE;

  public static enum NullHandling {
    /**
     * Method handles nulls.
     */
    INTERNAL,

    /**
     * Null output if any null input:
     * Indicates that a method's associated logical operation returns NULL if
     * either input is NULL, and therefore that the method must not be called
     * with null inputs.  (The calling framework must handle NULLs.)
     */
    NULL_IF_NULL;
  }

  public static enum FunctionScope {
    SIMPLE,
    POINT_AGGREGATE,
    DECIMAL_AGGREGATE,
    DECIMAL_SUM_AGGREGATE,
    HOLISTIC_AGGREGATE,
    RANGE_AGGREGATE,
    DECIMAL_MAX_SCALE,
    DECIMAL_MUL_SCALE,
    DECIMAL_CAST,
    DECIMAL_DIV_SCALE,
    DECIMAL_MOD_SCALE,
    DECIMAL_ADD_SCALE,
    DECIMAL_SET_SCALE,
    DECIMAL_ZERO_SCALE,
    SC_BOOLEAN_OPERATOR
  }

  public static enum FunctionCostCategory {
    SIMPLE(1), MEDIUM(20), COMPLEX(50);

    private final int value;

    private FunctionCostCategory(int value) {
      this.value = value;
    }

    public int getValue() {
      return this.value;
    }

    public static FunctionCostCategory getDefault() {
      return SIMPLE;
    }

  }
}
