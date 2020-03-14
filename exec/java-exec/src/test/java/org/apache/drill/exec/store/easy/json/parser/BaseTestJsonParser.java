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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.drill.exec.vector.accessor.UnsupportedConversionError;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonToken;

public class BaseTestJsonParser {

  @SuppressWarnings("serial")
  protected static class JsonErrorFixture extends RuntimeException {
    String errorType;

    public JsonErrorFixture(String errorType, String msg, Exception e) {
      super(msg, e);
      this.errorType = errorType;
    }

    public JsonErrorFixture(String errorType, String msg) {
      super(msg);
      this.errorType = errorType;
    }
  }

  /**
   * Convert JSON errors to a simple form for use in tests.
   * Not all errors are throw in normal operation; some require
   * faults in the I/O system or in the Jackson parser.
   */
  protected static class ErrorFactoryFixture implements ErrorFactory {

    @Override
    public RuntimeException parseError(String msg, JsonParseException e) {
      throw new JsonErrorFixture("parseError", msg, e);
    }

    @Override
    public RuntimeException ioException(IOException e) {
      throw new JsonErrorFixture("ioException", "", e);
    }

    @Override
    public RuntimeException structureError(String msg) {
      throw new JsonErrorFixture("structureError", msg);
    }

    @Override
    public RuntimeException syntaxError(JsonParseException e) {
      throw new JsonErrorFixture("syntaxError", "", e);
    }

    @Override
    public RuntimeException typeError(UnsupportedConversionError e) {
      throw new JsonErrorFixture("typeError", "", e);
    }

    @Override
    public RuntimeException syntaxError(JsonToken token) {
      throw new JsonErrorFixture("syntaxError", token.toString());
    }

    @Override
    public RuntimeException unrecoverableError() {
      throw new JsonErrorFixture("unrecoverableError", "");
    }
  }

  protected static class ValueListenerFixture implements ValueListener {

    final ValueDef valueDef;
    int nullCount;
    int valueCount;
    Object value;
    ValueHost host;
    ObjectListenerFixture objectValue;
    ArrayListenerFixture arrayValue;

    public ValueListenerFixture(ValueDef valueDef) {
      this.valueDef = valueDef;
    }

    @Override
    public void onNull() {
      nullCount++;
    }

    @Override
    public void onBoolean(boolean value) {
      this.value = value;
      valueCount++;
    }

    @Override
    public void onInt(long value) {
      this.value = value;
      valueCount++;
    }

    @Override
    public void onFloat(double value) {
      this.value = value;
      valueCount++;
    }

    @Override
    public void onString(String value) {
      this.value = value;
      valueCount++;
    }

    @Override
    public void onEmbeddedObject(String value) {
      this.value = value;
      valueCount++;
    }

    @Override
    public ObjectListener object() {
      assertNull(objectValue);
      objectValue = new ObjectListenerFixture();
      return objectValue;
    }

    @Override
    public ArrayListener array(ValueDef valueDef) {
      if (arrayValue == null) {
        arrayValue = new ArrayListenerFixture(valueDef);
      }
      return arrayValue;
    }

    @Override
    public void bind(ValueHost host) {
      this.host = host;
    }
  }

  protected static class ArrayListenerFixture implements ArrayListener {

    final ValueDef valueDef;
    int startCount;
    int endCount;
    int elementCount;
    ValueListenerFixture element;

    public ArrayListenerFixture(ValueDef valueDef) {
      this.valueDef = valueDef;
    }

    @Override
    public void onStart() {
      startCount++;
    }

    @Override
    public void onElementStart() {
      elementCount++;
    }

    @Override
    public void onElementEnd() { }

    @Override
    public void onEnd() {
      endCount++;
    }

    @Override
    public ValueListener element(ValueDef valueDef) {
      if (element == null) {
        element = new ValueListenerFixture(valueDef);
      }
      return element;
    }
  }

  protected static class ObjectListenerFixture implements ObjectListener {

    final Map<String, ValueListenerFixture> fields = new HashMap<>();
    Set<String> projectFilter;
    FieldType fieldType = FieldType.TYPED;
    int startCount;
    int endCount;

    @Override
    public void onStart() {
      startCount++;
    }

    @Override
    public void onEnd() {
      endCount++;
    }

    @Override
    public FieldType fieldType(String key) {
      if (projectFilter != null && !projectFilter.contains(key)) {
        return FieldType.IGNORE;
      }
      return fieldType;
    }

    @Override
    public ValueListener addField(String key, ValueDef valueDef) {
      assertFalse(fields.containsKey(key));
      ValueListenerFixture field = makeField(key, valueDef);
      fields.put(key, field);
      return field;
    }

    public ValueListenerFixture makeField(String key, ValueDef valueDef) {
      return new ValueListenerFixture(valueDef);
    }

    public ValueListenerFixture field(String key) {
      ValueListenerFixture field = fields.get(key);
      assertNotNull(field);
      return field;
    }
  }

  protected static class JsonParserFixture {
    JsonStructureOptions options = new JsonStructureOptions();
    JsonStructureParser parser;
    ObjectListenerFixture rootObject = new ObjectListenerFixture();
    ErrorFactory errorFactory = new ErrorFactoryFixture();

    public void open(String json) {
      InputStream inStream = new
          ReaderInputStream(new StringReader(json));
      parser = new JsonStructureParser(inStream, options, rootObject,
          errorFactory);
    }

    public boolean next() {
      assertNotNull(parser);
      return parser.next();
    }

    public int read() {
      int i = 0;
      while (next()) {
        i++;
      }
      return i;
    }

    public ValueListenerFixture field(String key) {
      return rootObject.field(key);
    }

    public void expect(String key, Object[] values) {
      ValueListenerFixture valueListener = null;
      int expectedNullCount = 0;
      for (int i = 0; i < values.length; i++) {
        assertTrue(next());
        if (valueListener == null) {
          valueListener = field(key);
          expectedNullCount = valueListener.nullCount;
        }
        Object value = values[i];
        if (value == null) {
          expectedNullCount++;
        } else {
          assertEquals(value, valueListener.value);
        }
        assertEquals(expectedNullCount, valueListener.nullCount);
      }
    }

    public void close() {
      if (parser != null) {
        parser.close();
      }
    }
  }

  protected static void expectError(String json, String kind) {
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.open(json);
    expectError(fixture, kind);
    fixture.close();
  }

  protected static void expectError(JsonParserFixture fixture, String kind) {
    try {
      fixture.read();
      fail();
    } catch (JsonErrorFixture e) {
      assertEquals(kind, e.errorType);
    }
  }
}
