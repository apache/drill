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
package org.apache.drill.exec.store.easy.json.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.exec.store.easy.json.parser.ValueDef.JsonType;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(RowSetTests.class)
public class TestJsonParserUnknowns extends BaseTestJsonParser {

  /**
   * Test replacing a value lister "in-flight". Handles the
   * (simulated) case where the initial value is ambiguous (here, null),
   * and a later token resolves the value to something concrete.
   */
  @Test
  public void testReplaceListener() {
    final String json = "{a: null} {a: true} {a: false}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);
    assertTrue(fixture.next());
    assertEquals(1, fixture.rootObject.startCount);
    assertEquals(1, fixture.rootObject.fields.size());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(JsonType.NULL, a.valueDef.type());
    assertEquals(0, a.valueDef.dimensions());
    assertEquals(1, a.nullCount);

    // Replace the listener with a new one
    ValueListenerFixture a2 = new ValueListenerFixture(new ValueDef(JsonType.BOOLEAN, a.valueDef.dimensions()));
    a.host.bindListener(a2);
    assertSame(a.host, a2.host);

    assertTrue(fixture.next());
    assertEquals(Boolean.TRUE, a2.value);
    assertTrue(fixture.next());
    assertEquals(0, a2.nullCount);
    assertEquals(Boolean.FALSE, a2.value);
    fixture.close();
  }

  private static class SwapperElementFixture extends ValueListenerFixture {

    private final SwapperListenerFixture parent;

    public SwapperElementFixture(SwapperListenerFixture parent, ValueDef valueDef) {
      super(valueDef);
      this.parent = parent;
    }

    @Override
    public void onInt(long value) {
      ValueListenerFixture newParent = parent.replace();
      newParent.array(ValueDef.UNKNOWN_ARRAY);
      newParent.arrayValue.element(ValueDef.UNKNOWN);
      newParent.arrayValue.element.onInt(value);
    }

  }
  private static class SwapperListenerFixture extends ValueListenerFixture {

    private final ObjectListenerFixture parent;
    private final String key;

    public SwapperListenerFixture(ObjectListenerFixture parent,
        String key, ValueDef valueDef) {
      super(valueDef);
      this.parent = parent;
      this.key = key;
    }

    @Override
    public void onInt(long value) {
      replace().onInt(value);
    }

    @Override
    public ObjectListener object() {
      return replace().object();
    }

    @Override
    public ArrayListener array(ValueDef valueDef) {
      if (valueDef.type() == JsonType.EMPTY) {
        super.array(valueDef);
        arrayValue.element = new SwapperElementFixture(this, ValueDef.UNKNOWN);
        return arrayValue;
      } else {
        return replace().array(valueDef);
      }
    }

    private ValueListenerFixture replace() {
      return replaceWith(new ValueListenerFixture(valueDef));
    }

    private ValueListenerFixture replaceWith(ValueListenerFixture newListener) {
      parent.fields.put(key, newListener);
      host.bindListener(newListener);
      return newListener;
    }
  }

  @Test
  public void testNullToScalar() {
    final String json =
        "{a: null} {a: 2} {a: 3}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.rootObject = new ObjectListenerFixture() {
      @Override
      public ValueListenerFixture makeField(String key, ValueDef valueDef) {
        return new SwapperListenerFixture(this, key, valueDef);
      }
    };
    fixture.open(json);

    // {a: null}
    assertTrue(fixture.next());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(JsonType.NULL, a.valueDef.type());
    assertEquals(0, a.valueDef.dimensions());
    assertNull(a.arrayValue);

    // See a scalar, can revise estimate of field type
    // {a: 2}
    assertTrue(fixture.next());
    ValueListenerFixture a2 = fixture.field("a");
    assertNotSame(a, a2);
    assertEquals(2L, a2.value);
    assertEquals(0, a2.nullCount);
    assertEquals(1, a2.valueCount);

    assertTrue(fixture.next());
    assertEquals(3L, a2.value);

    fixture.close();
  }

  @Test
  public void testNullToObject() {
    final String json =
        "{a: null} {a: {}} {a: {b: 3}}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.rootObject = new ObjectListenerFixture() {
      @Override
      public ValueListenerFixture makeField(String key, ValueDef valueDef) {
        return new SwapperListenerFixture(this, key, valueDef);
      }
    };
    fixture.open(json);

    // {a: null}
    assertTrue(fixture.next());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(1, a.nullCount);

    // See an object, can revise estimate of field type
    // {a: {}}
    assertTrue(fixture.next());
    ValueListenerFixture a2 = fixture.field("a");
    assertNotSame(a, a2);
    assertNotNull(a2.objectValue);
    assertTrue(a2.objectValue.fields.isEmpty());

    assertTrue(fixture.next());
    ValueListenerFixture b = a2.objectValue.field("b");
    assertEquals(3L, b.value);

    fixture.close();
  }

  @Test
  public void testNullToEmptyArray() {
    final String json =
        "{a: null} {a: []} {a: []} {a: [10, 20]} {a: [30, 40]}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.rootObject = new ObjectListenerFixture() {
      @Override
      public ValueListenerFixture makeField(String key, ValueDef valueDef) {
        return new SwapperListenerFixture(this, key, valueDef);
      }
    };
    fixture.open(json);

    // {a: null}
    assertTrue(fixture.next());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(1, a.nullCount);

    // See an empty array, can revise estimate of field type
    // {a: []}
    assertTrue(fixture.next());
    assertSame(a, fixture.field("a"));
    assertEquals(1, a.arrayValue.startCount);

    // Ensure things are stable
    // {a: []}
    assertTrue(fixture.next());
    assertSame(a, fixture.field("a"));
    assertEquals(2, a.arrayValue.startCount);

    // Revise again once we see element type
    // {a: [10, 20]}
    assertTrue(fixture.next());
    ValueListenerFixture a2 = fixture.field("a");
    assertNotSame(a, a2);
    assertEquals(3, a.arrayValue.startCount); // Start on old listener
    assertEquals(0, a2.arrayValue.startCount);
    assertEquals(1, a2.arrayValue.endCount); // End on new one
    assertEquals(2, a2.arrayValue.element.valueCount);
    assertEquals(20L, a2.arrayValue.element.value);

    // Check stability again
    // {a: [30, 40]}
    assertTrue(fixture.next());
    assertSame(a2, fixture.field("a"));
    assertEquals(1, a2.arrayValue.startCount);
    assertEquals(4, a2.arrayValue.element.valueCount);
    assertEquals(40L, a2.arrayValue.element.value);

    fixture.close();
  }

  /**
   * As above, but skips the intermediate empty array.
   * The array, when it appears, has type info.
   */
  @Test
  public void testNullToArray() {
    final String json =
        "{a: null} {a: [10, 20]} {a: [30, 40]}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.rootObject = new ObjectListenerFixture() {
      @Override
      public ValueListenerFixture makeField(String key, ValueDef valueDef) {
        return new SwapperListenerFixture(this, key, valueDef);
      }
    };
    fixture.open(json);

    // {a: null}
    assertTrue(fixture.next());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(1, a.nullCount);

    // See a typed empty array, can revise estimate of field type
    // {a: [10, 20]}
    assertTrue(fixture.next());
    ValueListenerFixture a2 = fixture.field("a");
    assertNotSame(a, a2);
    assertEquals(1, a2.arrayValue.startCount);
    assertEquals(1, a2.arrayValue.startCount);
    assertEquals(JsonType.INTEGER, a2.arrayValue.valueDef.type());
    assertEquals(1, a2.arrayValue.valueDef.dimensions());
    assertEquals(JsonType.INTEGER, a2.arrayValue.element.valueDef.type());
    assertEquals(0, a2.arrayValue.element.valueDef.dimensions());
    assertEquals(2, a2.arrayValue.element.valueCount);
    assertEquals(20L, a2.arrayValue.element.value);

    // Ensure things are stable
    // {a: [30, 40]}
    assertTrue(fixture.next());
    assertSame(a2, fixture.field("a"));
    assertEquals(2, a2.arrayValue.startCount);
    assertEquals(4, a2.arrayValue.element.valueCount);
    assertEquals(40L, a2.arrayValue.element.value);

    fixture.close();
  }
}
