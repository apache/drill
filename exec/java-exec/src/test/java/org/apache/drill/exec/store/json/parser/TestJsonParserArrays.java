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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.drill.exec.store.easy.json.parser.JsonType;
import org.junit.Test;

/**
 * Tests array (including multi-dimensional and object) support
 * for the JSON structure parser.
 */
public class TestJsonParserArrays extends BaseTestJsonParser {

  @Test
  public void test1DArray() {
    final String json =
        "{a: [1, 100]} {a: [null]} \n" +
        "{a: []} {a: null} {}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);

    // {a: [1, 100]}
    assertTrue(fixture.next());

    // Value of object.a
    ValueListenerFixture a = fixture.field("a");
    assertEquals(JsonType.INTEGER, a.type);
    assertEquals(1, a.dimCount);

    // Array aspect of a
    assertNotNull(a.arrayValue);
    ArrayListenerFixture aArray = a.arrayValue;
    assertEquals(1, aArray.startCount);
    assertEquals(aArray.startCount, aArray.endCount);
    assertEquals(1, aArray.dimCount);

    // Value of each element of array aspect of a
    assertNotNull(aArray.element);
    ValueListenerFixture aElement = aArray.element;
    assertEquals(JsonType.INTEGER, aElement.type);
    assertEquals(0, aElement.dimCount);
    assertNull(aElement.arrayValue);
    assertEquals(2, aElement.valueCount);
    assertEquals(100L, aElement.value);
    assertEquals(0, aElement.nullCount);

    // {a: [null]}
    assertTrue(fixture.next());
    assertEquals(2, aArray.startCount);
    assertEquals(aArray.startCount, aArray.endCount);
    assertEquals(2, aElement.valueCount);
    assertEquals(1, aElement.nullCount);

    // {a: []}
    assertTrue(fixture.next());
    assertEquals(3, aArray.startCount);
    assertEquals(aArray.startCount, aArray.endCount);
    assertEquals(2, aElement.valueCount);
    assertEquals(1, aElement.nullCount);
    assertEquals(0, a.nullCount);

    // {a: null}
    assertTrue(fixture.next());
    assertEquals(3, aArray.startCount);
    assertEquals(1, a.nullCount);

    // {}
    assertTrue(fixture.next());
    assertEquals(3, aArray.startCount);
    assertEquals(1, a.nullCount);

    assertFalse(fixture.next());
    fixture.close();
  }

  @Test
  public void testNullToArray() {
    final String json =
        "{a: null} {a: [1, 100]}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);

    // Can't predict the future, all we know is a is null.
    // {a: null}
    assertTrue(fixture.next());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(JsonType.NULL, a.type);
    assertEquals(0, a.dimCount);
    assertNull(a.arrayValue);

    // See an array, can revise estimate of field type
    // {a: [1, 100]}
    assertTrue(fixture.next());
    assertNotNull(a.arrayValue);
    ArrayListenerFixture aArray = a.arrayValue;
    assertEquals(1, aArray.dimCount);
    ValueListenerFixture aElement = aArray.element;
    assertEquals(2, aElement.valueCount);
    assertEquals(100L, aElement.value);

    assertFalse(fixture.next());
    fixture.close();
  }

  @Test
  public void testEmptyArray() {
    final String json =
        "{a: []} {a: [1, 100]}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);

    // Can't predict the future, all we know is a is an array.
    // "{a: []}
    assertTrue(fixture.next());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(JsonType.EMPTY, a.type);
    assertEquals(1, a.dimCount);
    assertNotNull(a.arrayValue);
    ArrayListenerFixture aArray = a.arrayValue;
    assertEquals(1, aArray.dimCount);
    ValueListenerFixture aElement = aArray.element;
    assertEquals(JsonType.EMPTY, aElement.type);

    // See elements, can revise estimate of element type
    // {a: [1, 100]}
    assertTrue(fixture.next());
    assertEquals(2, aElement.valueCount);
    assertEquals(100L, aElement.value);

    assertFalse(fixture.next());
    fixture.close();
  }

  @Test
  public void test2DArray() {
    final String json =
        "{a: [ [10, 1], [20, 2]]}\n" +
        "{a: [[null]]} {a: [[]]} {a: [null]} {a: null}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);

    // {a: [ [10, 1], [20, 2]]}
    assertTrue(fixture.next());

    // Value of a
    ValueListenerFixture a = fixture.field("a");
    assertEquals(JsonType.INTEGER, a.type);
    assertEquals(2, a.dimCount);

    // Array for a[]
    assertNotNull(a.arrayValue);
    ArrayListenerFixture outerArr = a.arrayValue;
    assertEquals(2, outerArr.dimCount);
    assertEquals(1, outerArr.startCount);
    assertEquals(outerArr.startCount, outerArr.endCount);

    // Value of a[] elements
    ValueListenerFixture outerElement = outerArr.element;
    assertEquals(JsonType.INTEGER, outerElement.type);
    assertEquals(1, outerElement.dimCount);
    assertNotNull(outerElement.arrayValue);

    // Array for a[][]
    assertNotNull(outerElement.arrayValue);
    ArrayListenerFixture innerArr = outerElement.arrayValue;
    assertEquals(1, innerArr.dimCount);
    assertEquals(2, innerArr.startCount);
    assertEquals(innerArr.startCount, innerArr.endCount);

    // Value of a[][] elements
    ValueListenerFixture innerElement = innerArr.element;
    assertEquals(JsonType.INTEGER, innerElement.type);
    assertEquals(0, innerElement.dimCount);
    assertEquals(4, innerElement.valueCount);
    assertEquals(0, innerElement.nullCount);
    assertEquals(2L, innerElement.value);

    // {a: [[null]]}
    assertTrue(fixture.next());
    assertEquals(2, outerArr.startCount);
    assertEquals(outerArr.startCount, outerArr.endCount);
    assertEquals(0, outerElement.nullCount);
    assertEquals(3, innerArr.startCount);
    assertEquals(innerArr.startCount, innerArr.endCount);
    assertEquals(4, innerElement.valueCount);
    assertEquals(1, innerElement.nullCount);

    // {a: [[]]}
    assertTrue(fixture.next());
    assertEquals(3, outerArr.startCount);
    assertEquals(outerArr.startCount, outerArr.endCount);
    assertEquals(0, outerElement.nullCount);
    assertEquals(4, innerArr.startCount);
    assertEquals(innerArr.startCount, innerArr.endCount);
    assertEquals(4, innerElement.valueCount);
    assertEquals(1, innerElement.nullCount);

    // {a: [null]}
    assertTrue(fixture.next());
    assertEquals(0, a.nullCount);
    assertEquals(4, outerArr.startCount);
    assertEquals(outerArr.startCount, outerArr.endCount);
    assertEquals(1, outerElement.nullCount);
    assertEquals(4, innerArr.startCount);
    assertEquals(4, innerElement.valueCount);
    assertEquals(1, innerElement.nullCount);

    // {a: null}
    assertTrue(fixture.next());
    assertEquals(1, a.nullCount);
    assertEquals(4, outerArr.startCount);
    assertEquals(outerArr.startCount, outerArr.endCount);
    assertEquals(1, outerElement.nullCount);
    assertEquals(4, innerArr.startCount);
    assertEquals(4, innerElement.valueCount);
    assertEquals(1, innerElement.nullCount);

    assertFalse(fixture.next());
    fixture.close();
  }

  @Test
  public void test1DEmptyTo2DArray() {
    final String json =
        "{a: []}\n" +
        "{a: [ [10, 1], [20, 2]]}\n";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);

    // Check of details done in other tests. Just cut to
    // the chase to verify proper structure.
    assertEquals(2, fixture.read());
    ValueListenerFixture element =
        fixture.field("a").arrayValue.element.arrayValue.element;
    assertEquals(4, element.valueCount);
    assertEquals(2L, element.value);

    assertFalse(fixture.next());
    fixture.close();
  }

  @Test
  public void testObjArray() {
    final String json =
        "{a: [ {b: \"fred\"}, {b: \"barney\"} ] }";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);

    assertTrue(fixture.next());

    // Value of object.a
    ValueListenerFixture a = fixture.field("a");
    assertEquals(JsonType.OBJECT, a.type);
    assertEquals(1, a.dimCount);

    // a[]
    assertNotNull(a.arrayValue);
    ArrayListenerFixture aArray = a.arrayValue;
    assertEquals(1, aArray.startCount);
    assertEquals(aArray.startCount, aArray.endCount);
    assertEquals(1, aArray.dimCount);

    // Value of each element of a[]
    assertNotNull(aArray.element);
    ValueListenerFixture aElement = aArray.element;
    assertEquals(JsonType.OBJECT, aElement.type);
    assertEquals(0, aElement.dimCount);
    assertNull(aElement.arrayValue);
    assertEquals(0, aElement.valueCount);
    assertEquals(0, aElement.nullCount);

    // Object for a[] elements
    assertNotNull(aElement.objectValue);
    ObjectListenerFixture elementObj = aElement.objectValue;
    assertEquals(2, elementObj.startCount);
    assertEquals(elementObj.startCount, elementObj.endCount);

    // b field within a[]{}
    ValueListenerFixture b = elementObj.field("b");
    assertEquals(2, b.valueCount);
    assertEquals("barney", b.value);

    assertFalse(fixture.next());
    fixture.close();
  }

  @Test
  public void test2DObjArray() {
    final String json =
        "{a: [ [ {b: \"fred\"}, {b: \"wilma\"} ],\n" +
        "      [ {b: \"barney\"}, {b: \"betty\"} ] ] }";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);

    assertTrue(fixture.next());

    // Value of object.a
    ValueListenerFixture a = fixture.field("a");
    assertEquals(JsonType.OBJECT, a.type);
    assertEquals(2, a.dimCount);

    // a[]
    assertNotNull(a.arrayValue);
    ArrayListenerFixture outerArray = a.arrayValue;
    assertEquals(1, outerArray.startCount);
    assertEquals(outerArray.startCount, outerArray.endCount);
    assertEquals(2, outerArray.dimCount);

    // Value of each element of a[]
    assertNotNull(outerArray.element);
    ValueListenerFixture outerElement = outerArray.element;
    assertEquals(JsonType.OBJECT, outerElement.type);
    assertEquals(1, outerElement.dimCount);
    assertEquals(0, outerElement.valueCount);
    assertEquals(0, outerElement.nullCount);

    // a[][]
    assertNotNull(outerElement.arrayValue);
    ArrayListenerFixture innerArray = outerElement.arrayValue;
    assertEquals(2, innerArray.startCount);
    assertEquals(innerArray.startCount, innerArray.endCount);
    assertEquals(1, innerArray.dimCount);

    // Value of each element of a[][]
    assertNotNull(innerArray.element);
    ValueListenerFixture innerElement = innerArray.element;
    assertEquals(JsonType.OBJECT, innerElement.type);
    assertEquals(0, innerElement.dimCount);
    assertEquals(0, innerElement.valueCount);
    assertEquals(0, innerElement.nullCount);

    // Object for a[][] elements
    assertNotNull(innerElement.objectValue);
    ObjectListenerFixture elementObj = innerElement.objectValue;
    assertEquals(4, elementObj.startCount);
    assertEquals(elementObj.startCount, elementObj.endCount);

    // b field within a[][]{}
    ValueListenerFixture b = elementObj.field("b");
    assertEquals(4, b.valueCount);
    assertEquals("betty", b.value);

    assertFalse(fixture.next());
  }

  /**
   * JSON allows any combination of value types.
   */
  @Test
  public void testMixArray() {
    final String json =
        "{a: [10, 11] }\n" +
        "{a: {b: \"fred\"}}\n" +
        "{a: 20.5}\n" +
        "{a: null}\n";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);

    assertEquals(4, fixture.read());
    ValueListenerFixture a = fixture.field("a");
    // Type first seen
    assertEquals(JsonType.INTEGER, a.type);
    assertEquals(1, a.dimCount);

    // Everything populated

    assertEquals(2, a.arrayValue.element.valueCount);
    assertEquals(11L, a.arrayValue.element.value);
    assertEquals(1, a.objectValue.startCount);
    assertEquals("fred", a.objectValue.field("b").value);
    assertEquals(20.5D, a.value);
    assertEquals(1, a.nullCount);

    fixture.close();
  }
}
