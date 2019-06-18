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
package org.apache.drill.metastore.expressions;

import java.util.StringJoiner;

/**
 * Indicates IS predicate implementations.
 */
public abstract class IsPredicate implements FilterExpression {

  private final String reference;
  private final Operator operator;

  protected IsPredicate(String reference, Operator operator) {
    this.reference = reference;
    this.operator = operator;
  }

  public String reference() {
    return reference;
  }

  @Override
  public Operator operator() {
    return operator;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", IsPredicate.class.getSimpleName() + "[", "]")
      .add("reference=" + reference)
      .add("operator=" + operator)
      .toString();
  }

  /**
   * Indicates {@link FilterExpression.Operator#IS_NULL} operator expression:
   * storagePlugin is null.
   */
  public static class IsNull extends IsPredicate {

    public IsNull(String reference) {
      super(reference, Operator.IS_NULL);
    }

    @Override
    public <V> V accept(Visitor<V> visitor) {
      return visitor.visit(this);
    }
  }

  /**
   * Indicates {@link FilterExpression.Operator#IS_NOT_NULL} operator expression:
   * storagePlugin is not null.
   */
  public static class IsNotNull extends IsPredicate {

    public IsNotNull(String reference) {
      super(reference, Operator.IS_NOT_NULL);
    }

    @Override
    public <V> V accept(Visitor<V> visitor) {
      return visitor.visit(this);
    }
  }
}
