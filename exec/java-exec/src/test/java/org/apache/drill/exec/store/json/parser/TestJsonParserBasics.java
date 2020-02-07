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
package org.apache.drill.exec.store.json.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;

import org.apache.drill.exec.store.easy.json.parser.JsonType;
import org.junit.Test;

/**
 * Tests JSON structure parser functionality excluding nested objects
 * and arrays. Tests the "happy path."
 */
public class TestJsonParserBasics extends BaseTestJsonParser {

  @Test
  public void testEmpty() {
    String json = "";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);
    assertFalse(fixture.next());
    assertEquals(0, fixture.rootObject.startCount);
    fixture.close();
  }

  @Test
  public void testEmptyTuple() {
    final String json = "{} {} {}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);
    assertEquals(3, fixture.read());
    assertEquals(3, fixture.rootObject.startCount);
    assertEquals(3, fixture.rootObject.endCount);
    assertTrue(fixture.rootObject.fields.isEmpty());
    fixture.close();
  }

  @Test
  public void testBoolean() {
    final String json = "{a: true} {a: false} {a: null}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);
    assertTrue(fixture.next());
    assertEquals(1, fixture.rootObject.startCount);
    assertEquals(1, fixture.rootObject.fields.size());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(JsonType.BOOLEAN, a.type);
    assertEquals(0, a.dimCount);
    assertEquals(0, a.nullCount);
    assertEquals(Boolean.TRUE, a.value);
    assertEquals(2, fixture.read());
    assertEquals(1, a.nullCount);
    assertEquals(Boolean.FALSE, a.value);
    fixture.close();
  }

  @Test
  public void testInteger() {
    final String json = "{a: 0} {a: 100} {a: null}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);
    assertTrue(fixture.next());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(JsonType.INTEGER, a.type);
    assertEquals(2, fixture.read());
    assertEquals(1, a.nullCount);
    assertEquals(100L, a.value);
    fixture.close();
  }

  @Test
  public void testFloat() {
    final String json = "{a: 0.0} {a: 100.5} {a: null}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);
    assertTrue(fixture.next());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(JsonType.FLOAT, a.type);
    assertEquals(2, fixture.read());
    assertEquals(1, a.nullCount);
    assertEquals(2, a.valueCount);
    assertEquals(100.5D, a.value);
    fixture.close();
  }

  @Test
  public void testExtendedFloat() {
    final String json =
        "{a: NaN} {a: Infinity} {a: -Infinity}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.options.allowNanInf = true;
    fixture.open(json);
    assertTrue(fixture.next());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(JsonType.FLOAT, a.type);
    assertEquals(2, fixture.read());
    assertEquals(3, a.valueCount);
    assertEquals(Double.NEGATIVE_INFINITY, a.value);
    fixture.close();
  }

  @Test
  public void testString() {
    final String json = "{a: \"\"} {a: \"hi\"} {a: null}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);
    assertTrue(fixture.next());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(JsonType.STRING, a.type);
    assertEquals(2, fixture.read());
    assertEquals(1, a.nullCount);
    assertEquals(2, a.valueCount);
    assertEquals("hi", a.value);
    fixture.close();
  }

  @Test
  public void testMixedTypes() {
    final String json = "{a: \"hi\"} {a: 10} {a: 10.5}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);
    assertTrue(fixture.next());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(JsonType.STRING, a.type);
    assertEquals("hi", a.value);
    assertTrue(fixture.next());
    assertEquals(10L, a.value);
    assertTrue(fixture.next());
    assertEquals(10.5D, a.value);
    assertFalse(fixture.next());
    fixture.close();
  }

  @Test
  public void testRootTuple() {
    final String json =
      "{id: 1, name: \"Fred\", balance: 100.0}\n" +
      "{id: 2, name: \"Barney\"}\n" +
      "{id: 3, name: \"Wilma\", balance: 500.00}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);
    assertEquals(3, fixture.read());

    ValueListenerFixture name = fixture.field("name");
    assertEquals(3, name.valueCount);
    assertEquals("Wilma", name.value);
    ValueListenerFixture balance = fixture.field("balance");
    assertEquals(2, balance.valueCount);
    assertEquals(500.00D, balance.value);
    fixture.close();
  }

  @Test
  public void testRootArray() {
    final String json = "[{a: 0}, {a: 100}, {a: null}]";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);
    assertEquals(3, fixture.read());

    ValueListenerFixture a = fixture.field("a");
    assertEquals(2, a.valueCount);
    assertEquals(1, a.nullCount);
    fixture.close();
  }

  @Test
  public void testLeadingTrailingWhitespace() {
    final String json = "{\" a\": 10, \" b\": 20, \" c \": 30}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);
    assertEquals(1, fixture.read());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(10L, a.value);
    ValueListenerFixture b = fixture.field("b");
    assertEquals(20L, b.value);
    ValueListenerFixture c = fixture.field("c");
    assertEquals(30L, c.value);
    fixture.close();
  }

  /**
   * Verify that names are case insensitive, first name determine's
   * Drill's column name.
   */
  @Test
  public void testCaseInsensitive() {
    final String json = "{a: 10} {A: 20} {\" a \": 30}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);
    assertEquals(3, fixture.read());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(3, a.valueCount);
    assertEquals(30L, a.value);
    fixture.close();
  }

  /**
   * Verify that the first name wins when determining case.
   */
  @Test
  public void testMixedCase() {
    final String json = "{Bob: 10} {bOb: 20} {BoB: 30}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);
    assertEquals(3, fixture.read());
    ValueListenerFixture bob = fixture.field("Bob");
    assertEquals(3, bob.valueCount);
    assertEquals(30L, bob.value);
    fixture.close();
  }

  @Test
  public void testProjection() {
    final String json =
        "{a: 1, b: [[{x: [[{y: []}]]}]]}\n" +
        "{a: 2}\n" +
        "{b: \"bar\"}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.rootObject.projectFilter = new HashSet<>();
    fixture.rootObject.projectFilter.add("a");
    fixture.open(json);

    assertEquals(3, fixture.read());
    assertEquals(1, fixture.rootObject.fields.size());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(2, a.valueCount);
    assertEquals(2L, a.value);
    fixture.close();
  }

  @Test
  public void testAllTextMode() {
    final String json =
        "{a: 1} {a: \"foo\"} {a: true} {a: 20.5} {a: null}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.options.allTextMode = true;
    fixture.open(json);

    assertTrue(fixture.next());
    ValueListenerFixture a = fixture.field("a");
    assertEquals("1", a.value);

    assertTrue(fixture.next());
    assertEquals("foo", a.value);

    assertTrue(fixture.next());
    assertEquals("true", a.value);

    assertTrue(fixture.next());
    assertEquals("20.5", a.value);
    assertEquals(0, a.nullCount);

    assertTrue(fixture.next());
    assertEquals("20.5", a.value);
    assertEquals(1, a.nullCount);

    assertFalse(fixture.next());
    fixture.close();
  }
}
