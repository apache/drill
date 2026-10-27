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
package org.apache.drill.exec.planner.sql;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.drill.common.expression.ExpressionStringBuilder;
import org.apache.drill.common.expression.LiteralExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.parser.LogicalExpressionParser;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.server.options.QueryOptionManager;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.test.BaseTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Exercises the real JSON/expression binder independently of scan reconstruction. */
public class TestPlanCacheRebind extends BaseTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final SqlParserPos POS = SqlParserPos.ZERO;
  private final AtomicReference<String> decodedJson = new AtomicReference<>();
  private PlanCache cache;
  private PhysicalPlanReader reader;
  private PlanCache.ContextSnapshot snapshot;

  @Before
  public void setup() throws Exception {
    QueryContext context = mock(QueryContext.class);
    QueryOptionManager options = mock(QueryOptionManager.class);
    when(options.iterator()).thenAnswer(call -> Collections.emptyIterator());
    when(context.getOptions()).thenReturn(options);
    when(context.getStorage()).thenReturn(mock(StoragePluginRegistry.class));
    snapshot = PlanCache.ContextSnapshot.resolve(mock(SchemaPlus.class),
        SqlParser.create("SELECT 1").parseQuery(), context);
    reader = mock(PhysicalPlanReader.class);
    when(reader.readPhysicalPlan(anyString())).thenAnswer(call -> {
      decodedJson.set(call.getArgument(0));
      return null;
    });
    when(reader.readPhysicalPlanForCache(anyString())).thenAnswer(call -> {
      decodedJson.set(call.getArgument(0));
      return null;
    });
    cache = new PlanCache(1024 * 1024, Duration.ZERO, Duration.ZERO);
  }

  @After
  public void close() {
    cache.close();
  }

  @Test
  public void testRebindPreservesEverySupportedLiteralTypeAndSlot() throws Exception {
    List<LogicalExpression> original = Arrays.asList(
        ValueExpressions.getBit(false), ValueExpressions.getInt(1), ValueExpressions.getBigInt(2147483648L),
        ValueExpressions.getFloat4(1.25F), ValueExpressions.getFloat8(1.25D),
        ValueExpressions.getVarDecimal(new BigDecimal("10.00"), 6, 2), ValueExpressions.getChar("old", 40));
    List<LogicalExpression> expected = Arrays.asList(
        ValueExpressions.getBit(true), ValueExpressions.getInt(2), ValueExpressions.getBigInt(9223372036854775806L),
        ValueExpressions.getFloat4(2.5F), ValueExpressions.getFloat8(3.5D),
        ValueExpressions.getVarDecimal(new BigDecimal("12.34"), 6, 2),
        ValueExpressions.getChar("new 'value'", 40));
    String[] expressions = new String[original.size()];
    for (int i = 0; i < original.size(); i++) {
      ((LiteralExpression) original.get(i)).setDynamicParamIndex(i);
      ((LiteralExpression) expected.get(i)).setDynamicParamIndex(i);
      expressions[i] = ExpressionStringBuilder.toString(original.get(i));
    }
    PlanCache.Entry entry = entry(JSON.writeValueAsString(Collections.singletonMap("expressions", expressions)),
        Arrays.asList(SqlLiteral.createBoolean(false, POS), number("1"), number("2147483648"),
            number("1.25"), number("1.25"), number("10.00"), SqlLiteral.createCharString("old", POS)));
    entry.bind(Arrays.asList(SqlLiteral.createBoolean(true, POS), number("2"), number("9223372036854775806"),
        number("2.5"), number("3.5"), number("12.34"), SqlLiteral.createCharString("new 'value'", POS)), reader);
    JsonNode rebound = JSON.readTree(decodedJson.get()).get("expressions");
    for (int i = 0; i < expected.size(); i++) {
      LogicalExpression actual = LogicalExpressionParser.parse(rebound.get(i).asText());
      assertEquals(expected.get(i).getMajorType(), actual.getMajorType());
      assertEquals(i, ((LiteralExpression) actual).getDynamicParamIndex());
      assertEquals(ExpressionStringBuilder.toString(expected.get(i)), ExpressionStringBuilder.toString(actual));
    }
  }

  @Test
  public void testRepeatedRebindsLeaveTemplateImmutableAndVisitNestedArrays() throws Exception {
    String expression = "equal(bound_dynamic_param(0, INT, 1), bound_dynamic_param(0, INT, 1))";
    String json = JSON.writeValueAsString(Collections.singletonMap("nested",
        Arrays.asList(Collections.singletonMap("condition", expression), Collections.singletonList(expression))));
    PlanCache.Entry entry = entry(json, Collections.singletonList(number("1")));
    for (String value : Arrays.asList("2", "3", "2")) {
      entry.bind(Collections.singletonList(number(value)), reader);
      JsonNode bound = JSON.readTree(decodedJson.get()).get("nested");
      LogicalExpression actual = LogicalExpressionParser.parse(bound.get(0).get("condition").asText());
      String expected = expression.replace("INT, 1", "INT, " + value);
      assertEquals(canonical(expected), ExpressionStringBuilder.toString(actual));
      assertEquals(bound.get(0).get("condition").asText(), bound.get(1).get(0).asText());
    }
    entry.bind(Collections.singletonList(number("1")), reader);
    assertEquals(canonical(expression),
        canonical(JSON.readTree(decodedJson.get()).get("nested").get(0).get("condition").asText()));
  }

  @Test
  public void testMarkerInsideQuotedValueAndFieldNameIsNotRebound() throws Exception {
    String expression = "equal(`bound_dynamic_param(`, 'bound_dynamic_param(7, INT, 9)')";
    PlanCache.Entry entry = entry(JSON.writeValueAsString(Collections.singletonMap("expression", expression)));
    entry.bind(Collections.emptyList(), reader);
    assertEquals(expression, JSON.readTree(decodedJson.get()).get("expression").asText());
  }

  @Test
  public void testInvalidSlotAndNumericOverflowAreRejected() throws Exception {
    PlanCache.Entry entry = entry("{\"expression\":\"bound_dynamic_param(0, INT, 1)\"}",
        Collections.singletonList(number("1")));
    assertThrows(IllegalArgumentException.class, () -> entry.bind(Collections.emptyList(), reader));
    assertThrows(ArithmeticException.class,
        () -> entry.bind(Collections.singletonList(number("2147483648")), reader));
    entry.bind(Collections.singletonList(number("2")), reader);
    assertEquals(canonical("bound_dynamic_param(0, INT, 2)"),
        canonical(JSON.readTree(decodedJson.get()).get("expression").asText()));
  }

  @Test
  public void testDecimalPrecisionAndScaleCannotChange() throws Exception {
    PlanCache.Entry entry = entry("{\"expression\":\"bound_dynamic_param(0, VARDECIMAL(4, 2), 1.00)\"}",
        Collections.singletonList(number("1.00")));
    assertThrows(IllegalArgumentException.class,
        () -> entry.bind(Collections.singletonList(number("123.45")), reader));
    assertThrows(ArithmeticException.class,
        () -> entry.bind(Collections.singletonList(number("1.234")), reader));
  }

  @Test
  public void testTrialBindRejectsMarkerInNonExpressionTextBeforePublication() throws Exception {
    String json = "{\"description\":\"not an expression: bound_dynamic_param(\"}";
    assertThrows(RuntimeException.class, () -> entry(json));
    assertNull(cache.get("key"));
    // The background writer also skips this entry, so a later lookup cannot
    // repeatedly hit an unusable template and invalidate it again.
    cache.writeAfterSuccess("key", json, null, reader, snapshot, cache.getGeneration(), Collections.emptyList());
    cache.awaitWrites();
    assertNull(cache.get("key"));
  }

  private PlanCache.Entry entry(String json) throws Exception {
    return entry(json, Collections.emptyList());
  }

  private PlanCache.Entry entry(String json, List<SqlLiteral> literals) throws Exception {
    assertTrue(cache.put("key", json, null, reader, snapshot, literals));
    return cache.get("key");
  }

  private static SqlLiteral number(String value) {
    return SqlLiteral.createExactNumeric(value, POS);
  }

  private static String canonical(String expression) {
    return ExpressionStringBuilder.toString(LogicalExpressionParser.parse(expression));
  }
}
