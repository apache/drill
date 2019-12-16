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
package org.apache.drill.exec.store.base.filter;

import org.apache.drill.exec.store.base.PlanStringBuilder;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Semanticized form of a Calcite relational operator. Abstracts
 * out the Drill implementation details to capture just the
 * column name, operator and value. Supports only expressions
 * of the form:<br>
 * <code>&lt;column> &lt;relop> &lt;const></code><br>
 * Where the column is a simple name (not an array or map reference),
 * the relop is one of a defined set, and the constant is one
 * of the defined Drill types.
 * <p>
 * (The driver will convert expressions of the form:<br>
 * <code>&lt;const></code> &lt;relop> <code>&lt;column></code><br>
 * into the normalized form represented here.
 */

@JsonInclude(Include.NON_NULL)
@JsonPropertyOrder({"op", "colName", "value"})
public class RelOp {

  public enum Op {
    EQ, NE, LT, LE, GT, GE, IS_NULL, IS_NOT_NULL;

    /**
     * Return the result of flipping the sides of an
     * expression:</br>
     * a op b &rarr; b op.invert() a
     */
    public RelOp.Op invert() {
      switch(this) {
      case LT:
        return GT;
      case LE:
        return GE;
      case GT:
        return LT;
      case GE:
        return LE;
      default:
        return this;
      }
    }

    public int argCount() {
      switch (this) {
      case IS_NULL:
      case IS_NOT_NULL:
        return 1;
      default:
        return 2;
      }
    }

    /**
     * Poor-man's guess at selectivity of each operator.
     * Should match Calcite's built-in defaults (which are
     * hard to find.)
     *
     * TODO: Double check against Drill defaults.
     * @return crude estimate of operator selectivity
     */

    public double selectivity() {
      switch (this) {
      case EQ:
        return 0.15;
      case GE:
      case GT:
      case LE:
      case LT:
        return 0.45;
      case IS_NOT_NULL:
      case IS_NULL:
        return 0.5;
      case NE:
        return 0.85;
      default:
        return 0.5;
      }
    }
  }

  @JsonProperty("op")
  public final RelOp.Op op;
  @JsonProperty("colName")
  public final String colName;
  @JsonProperty("value")
  public final ConstantHolder value;

  @JsonCreator
  public RelOp(
      @JsonProperty("op") RelOp.Op op,
      @JsonProperty("colName") String colName,
      @JsonProperty("value") ConstantHolder value) {
    Preconditions.checkArgument(op.argCount() == 1 || value != null);
    this.op = op;
    this.colName = colName;
    this.value = value;
  }

  /**
   * Rewrite the RelOp with a normalized value.
   *
   * @param from the original RelOp
   * @param value the new value with a different type and matching
   * value
   */

  public RelOp(RelOp from, ConstantHolder value) {
    Preconditions.checkArgument(from.op.argCount() == 2);
    this.op = from.op;
    this.colName = from.colName;
    this.value = value;
  }

  /**
   * Return a new RelOp with the normalized value. Will be the same relop
   * if the normalized value is the same as the unnormalized value.
   */

  public RelOp normalize(ConstantHolder normalizedValue) {
    if (value == normalizedValue) {
      return this;
    }
    return new RelOp(this, normalizedValue);
  }

  public RelOp rewrite(String newName, ConstantHolder newValue) {
    if (value == newValue && colName.equals(newName)) {
      return this;
    }
    return new RelOp(op, newName, newValue);
  }

  @Override
  public String toString() {
    PlanStringBuilder builder = new PlanStringBuilder(this)
      .field("op", op.name())
      .field("colName", colName);
    if (value != null) {
      builder.field("type", value.type.name())
             .field("value", value.value);
    }
    return builder.toString();
  }
}
