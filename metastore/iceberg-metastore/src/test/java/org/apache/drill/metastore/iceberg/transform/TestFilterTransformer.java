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
package org.apache.drill.metastore.iceberg.transform;

import org.apache.drill.metastore.expressions.FilterExpression;
import org.apache.drill.metastore.iceberg.IcebergBaseTest;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestFilterTransformer extends IcebergBaseTest {

  private static FilterTransformer transformer;

  @BeforeClass
  public static void init() {
    transformer = new FilterTransformer();
  }

  @Test
  public void testToFilterNull() {
    Expression expected = Expressions.alwaysTrue();
    Expression actual = transformer.transform((FilterExpression) null);

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterEqual() {
    Expression expected = Expressions.equal("a", 1);
    Expression actual = transformer.transform(FilterExpression.equal("a", 1));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterNotEqual() {
    Expression expected = Expressions.notEqual("a", 1);
    Expression actual = transformer.transform(FilterExpression.notEqual("a", 1));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterLessThan() {
    Expression expected = Expressions.lessThan("a", 1);
    Expression actual = transformer.transform(FilterExpression.lessThan("a", 1));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterLessThanOrEqual() {
    Expression expected = Expressions.lessThanOrEqual("a", 1);
    Expression actual = transformer.transform(FilterExpression.lessThanOrEqual("a", 1));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterGreaterThan() {
    Expression expected = Expressions.greaterThan("a", 1);
    Expression actual = transformer.transform(FilterExpression.greaterThan("a", 1));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterGreaterThanOrEqual() {
    Expression expected = Expressions.greaterThanOrEqual("a", 1);
    Expression actual = transformer.transform(FilterExpression.greaterThanOrEqual("a", 1));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterIn() {
    Expression expected = Expressions.or(Expressions.equal("a", 1), Expressions.equal("a", 2));
    Expression actual = transformer.transform(FilterExpression.in("a", 1, 2));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterNotIn() {
    Expression expected = Expressions.not(
      Expressions.or(Expressions.equal("a", 1), Expressions.equal("a", 2)));
    Expression actual = transformer.transform(FilterExpression.notIn("a", 1, 2));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterIsNull() {
    Expression expected = Expressions.isNull("a");
    Expression actual = transformer.transform(FilterExpression.isNull("a"));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterIsNotNull() {
    Expression expected = Expressions.notNull("a");
    Expression actual = transformer.transform(FilterExpression.isNotNull("a"));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterNot() {
    Expression expected = Expressions.not(Expressions.equal("a", 1));
    Expression actual = transformer.transform(FilterExpression.not(FilterExpression.equal("a", 1)));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterAnd() {
    Expression expected = Expressions.and(
      Expressions.equal("a", 1), Expressions.equal("b", 2),
      Expressions.equal("c", 3), Expressions.equal("d", 4));

    Expression actual = transformer.transform(FilterExpression.and(
      FilterExpression.equal("a", 1), FilterExpression.equal("b", 2),
      FilterExpression.equal("c", 3), FilterExpression.equal("d", 4)));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterOr() {
    Expression expected = Expressions.or(Expressions.equal("a", 1), Expressions.equal("a", 2));
    Expression actual = transformer.transform(
      FilterExpression.or(FilterExpression.equal("a", 1), FilterExpression.equal("a", 2)));

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testToFilterUnsupported() {
    thrown.expect(UnsupportedOperationException.class);

    transformer.transform(new FilterExpression() {
      @Override
      public Operator operator() {
        return null;
      }

      @Override
      public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
      }
    });
  }

  @Test
  public void testToFilterConditionsNull() {
    assertEquals(Expressions.alwaysTrue().toString(), transformer.transform((Map<String, Object>) null).toString());
  }

  @Test
  public void testToFilterConditionsEmpty() {
    assertEquals(Expressions.alwaysTrue().toString(), transformer.transform(Collections.emptyMap()).toString());
  }

  @Test
  public void testToFilterConditionsOne() {
    Map<String, Object> conditions = new HashMap<>();
    conditions.put("a", 1);

    assertEquals(Expressions.equal("a", 1).toString(), transformer.transform(conditions).toString());
  }

  @Test
  public void testToFilterConditionsTwo() {
    Map<String, Object> conditions = new HashMap<>();
    conditions.put("a", 1);
    conditions.put("b", 2);

    Expression expected = Expressions.and(
      Expressions.equal("a", 1), Expressions.equal("b", 2));

    assertEquals(expected.toString(), transformer.transform(conditions).toString());
  }

  @Test
  public void testToFilterConditionsFour() {
    Map<String, Object> conditions = new HashMap<>();
    conditions.put("a", 1);
    conditions.put("b", 2);
    conditions.put("c", 3);
    conditions.put("d", 4);

    Expression expected = Expressions.and(
      Expressions.equal("a", 1), Expressions.equal("b", 2),
      Expressions.equal("c", 3), Expressions.equal("d", 4));

    assertEquals(expected.toString(), transformer.transform(conditions).toString());
  }
}
