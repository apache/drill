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
 * Tests nested object support in the JSON structure parser.
 */
public class TestJsonParserObjects extends BaseTestJsonParser {

  @Test
  public void testNestedTuple() {
    final String json =
        "{id: 1, customer: { name: \"fred\" }}\n" +
        "{id: 2, customer: { name: \"barney\" }}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);

    assertTrue(fixture.next());
    assertEquals(1, fixture.rootObject.startCount);
    assertEquals(fixture.rootObject.startCount, fixture.rootObject.endCount);
    ValueListenerFixture cust = fixture.field("customer");
    assertNotNull(cust.objectValue);
    ObjectListenerFixture custObj = cust.objectValue;
    assertEquals(1, custObj.startCount);
    assertEquals(custObj.startCount, custObj.endCount);
    ValueListenerFixture name = custObj.field("name");
    assertEquals(JsonType.STRING, name.type);
    assertEquals("fred", name.value);

    assertTrue(fixture.next());
    assertEquals(2, fixture.rootObject.startCount);
    assertEquals(fixture.rootObject.startCount, fixture.rootObject.endCount);
    assertEquals("barney", name.value);

    assertFalse(fixture.next());
    fixture.close();
  }

  @Test
  public void testObjectToNull() {
    final String json =
        "{id: 1, customer: {name: \"fred\"}}\n" +
        "{id: 2, customer: null}\n" +
        "{id: 3}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);

    assertTrue(fixture.next());
    ValueListenerFixture cust = fixture.field("customer");
    assertEquals(0, cust.valueCount);
    assertEquals(0, cust.nullCount);
    ObjectListenerFixture custObj = cust.objectValue;
    assertEquals(1, custObj.startCount);
    assertEquals(custObj.startCount, custObj.endCount);
    ValueListenerFixture name = custObj.field("name");
    assertEquals("fred", name.value);

    assertTrue(fixture.next());
    assertEquals(1, cust.nullCount);
    assertEquals(1, custObj.startCount);
    assertEquals(custObj.startCount, custObj.endCount);

    assertTrue(fixture.next());
    assertEquals(1, cust.nullCount);
    assertEquals(1, custObj.startCount);
    assertEquals(custObj.startCount, custObj.endCount);

    assertFalse(fixture.next());
    fixture.close();
  }

  @Test
  public void testNullToObject() {
    final String json =
        "{id: 1}\n" +
        "{id: 2, customer: null}\n" +
        "{id: 3, customer: {name: \"fred\"}}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);

    assertTrue(fixture.next());
    assertEquals(1, fixture.rootObject.fields.size());

    assertTrue(fixture.next());
    ValueListenerFixture cust = fixture.field("customer");
    assertEquals(0, cust.valueCount);
    assertEquals(1, cust.nullCount);
    assertNull(cust.objectValue);

    assertTrue(fixture.next());
    assertNotNull(cust.objectValue);
    ObjectListenerFixture custObj = cust.objectValue;
    assertEquals(1, custObj.startCount);
    assertEquals(custObj.startCount, custObj.endCount);
    ValueListenerFixture name = custObj.field("name");
    assertEquals("fred", name.value);

    assertFalse(fixture.next());
    fixture.close();
  }

  @Test
  public void testMixedObject() {
    final String json =
        "{id: 1, customer: null}\n" +
        "{id: 2, customer: {name: \"fred\"}}\n" +
        "{id: 3, customer: 123}\n" +
        "{id: 4, customer: {name: \"barney\"}}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);

    assertTrue(fixture.next());

    assertTrue(fixture.next());
    ValueListenerFixture cust = fixture.field("customer");
    assertNotNull(cust.objectValue);
    ObjectListenerFixture custObj = cust.objectValue;
    ValueListenerFixture name = custObj.field("name");
    assertEquals("fred", name.value);

    assertTrue(fixture.next());
    assertEquals(1, cust.valueCount);
    assertEquals(123L, cust.value);

    assertTrue(fixture.next());
    assertNotNull(cust.objectValue);
    assertEquals(2, custObj.startCount);
    assertEquals(custObj.startCount, custObj.endCount);
    assertEquals("barney", name.value);

    assertFalse(fixture.next());
    fixture.close();
  }
}
