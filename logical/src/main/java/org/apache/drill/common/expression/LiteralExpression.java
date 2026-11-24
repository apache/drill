/*
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
package org.apache.drill.common.expression;

import com.fasterxml.jackson.annotation.JsonIgnore;

/** A literal that may carry a bound SQL parameter slot in a cached plan. */
public abstract class LiteralExpression extends LogicalExpressionBase {
  private int dynamicParamIndex = -1;

  protected LiteralExpression(ExpressionPosition position) {
    super(position);
  }

  @JsonIgnore
  public int getDynamicParamIndex() {
    return dynamicParamIndex;
  }

  /** Whether this literal is bound to a zero-based SQL parameter slot. */
  @JsonIgnore
  public boolean isDynamicParam() {
    return dynamicParamIndex >= 0;
  }

  public void setDynamicParamIndex(int dynamicParamIndex) {
    if (dynamicParamIndex < 0) {
      throw new IllegalArgumentException("Dynamic parameter index must be non-negative");
    }
    this.dynamicParamIndex = dynamicParamIndex;
  }
}
