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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Interface which defines filter expression types by which Metastore data can be read or deleted.
 */
public interface FilterExpression {

  Operator operator();

  <T> T accept(Visitor<T> visitor);

  /**
   * Indicates list of supported operators that can be used in filter expressions.
   */
  enum Operator {
    EQUAL,
    NOT_EQUAL,
    LESS_THAN,
    LESS_THAN_OR_EQUAL,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL,
    IN,
    NOT_IN,
    IS_NULL,
    IS_NOT_NULL,
    NOT,
    AND,
    OR
  }

  /**
   * Transforms {@link FilterExpression} implementations into suitable
   * for Metastore implementation representation.
   * Is handy when needed to traverse through complex filter expressions.
   *
   * @param <T> type into which {@link FilterExpression} will be transformed
   */
  interface Visitor<T> {

    T visit(SimplePredicate.Equal<?> expression);
    T visit(SimplePredicate.NotEqual<?> expression);
    T visit(SimplePredicate.LessThan<?> expression);
    T visit(SimplePredicate.LessThanOrEqual<?> expression);
    T visit(SimplePredicate.GreaterThan<?> expression);
    T visit(SimplePredicate.GreaterThanOrEqual<?> expression);
    T visit(ListPredicate.In<?> expression);
    T visit(ListPredicate.NotIn<?> expression);
    T visit(IsPredicate.IsNull expression);
    T visit(IsPredicate.IsNotNull expression);
    T visit(SingleExpressionPredicate.Not expression);
    T visit(DoubleExpressionPredicate.And expression);
    T visit(DoubleExpressionPredicate.Or expression);

    default T visit(FilterExpression expression) {
      throw new UnsupportedOperationException("Unsupported filter expression: " + expression);
    }
  }

  static <T> FilterExpression equal(String reference, T value) {
    return new SimplePredicate.Equal<>(reference, value);
  }

  static <T> FilterExpression notEqual(String reference, T value) {
    return new SimplePredicate.NotEqual<>(reference, value);
  }

  static <T> FilterExpression lessThan(String reference, T value) {
    return new SimplePredicate.LessThan<>(reference, value);
  }

  static <T> FilterExpression lessThanOrEqual(String reference, T value) {
    return new SimplePredicate.LessThanOrEqual<>(reference, value);
  }

  static <T> FilterExpression greaterThan(String reference, T value) {
    return new SimplePredicate.GreaterThan<>(reference, value);
  }

  static <T> FilterExpression greaterThanOrEqual(String reference, T value) {
    return new SimplePredicate.GreaterThanOrEqual<>(reference, value);
  }

  static <T> FilterExpression in(String reference, List<T> values) {
    return new ListPredicate.In<>(reference, values);
  }

  @SafeVarargs
  static <T> FilterExpression in(String reference, T... values) {
    return in(reference, Arrays.asList(values));
  }

  static <T> FilterExpression notIn(String reference, List<T> values) {
    return new ListPredicate.NotIn<>(reference, values);
  }

  @SafeVarargs
  static <T> FilterExpression notIn(String reference, T... values) {
    return notIn(reference, Arrays.asList(values));
  }

  static FilterExpression isNull(String reference) {
    return new IsPredicate.IsNull(reference);
  }

  static FilterExpression isNotNull(String reference) {
    return new IsPredicate.IsNotNull(reference);
  }

  static FilterExpression not(FilterExpression expression) {
    return new SingleExpressionPredicate.Not(expression);
  }

  static FilterExpression and(FilterExpression right, FilterExpression left) {
    return new DoubleExpressionPredicate.And(right, left);
  }

  static FilterExpression and(FilterExpression right, FilterExpression left, FilterExpression... expressions) {
    return Stream.of(expressions)
      .reduce(and(right, left), FilterExpression::and);
  }

  static FilterExpression or(FilterExpression right, FilterExpression left) {
    return new DoubleExpressionPredicate.Or(right, left);
  }
}
