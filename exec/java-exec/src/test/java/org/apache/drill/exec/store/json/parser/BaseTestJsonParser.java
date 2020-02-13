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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.drill.exec.store.easy.json.parser.ArrayListener;
import org.apache.drill.exec.store.easy.json.parser.ErrorFactory;
import org.apache.drill.exec.store.easy.json.parser.JsonStructureOptions;
import org.apache.drill.exec.store.easy.json.parser.JsonStructureParser;
import org.apache.drill.exec.store.easy.json.parser.JsonType;
import org.apache.drill.exec.store.easy.json.parser.ObjectListener;
import org.apache.drill.exec.store.easy.json.parser.ValueListener;
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

    final int dimCount;
    final JsonType type;
    int nullCount;
    int valueCount;
    Object value;
    ObjectListenerFixture objectValue;
    ArrayListenerFixture arrayValue;

    public ValueListenerFixture(int dimCount, JsonType type) {
      this.dimCount = dimCount;
      this.type = type;
    }

    @Override
    public boolean isText() { return false; }

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
    public void onEmbedddObject(String value) {
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
    public ArrayListener array(int arrayDims, JsonType type) {
      assertNull(arrayValue);
      arrayValue = new ArrayListenerFixture(arrayDims, type);
      return arrayValue;
    }

    @Override
    public ArrayListener objectArray(int arrayDims) {
      assertNull(arrayValue);
      arrayValue = new ArrayListenerFixture(arrayDims, JsonType.OBJECT);
      return arrayValue;
    }
  }

  protected static class ArrayListenerFixture implements ArrayListener {

    final int dimCount;
    final JsonType type;
    int startCount;
    int endCount;
    int elementCount;
    ValueListenerFixture element;

    public ArrayListenerFixture(int dimCount, JsonType type) {
      this.dimCount = dimCount;
      this.type = type;
    }

    @Override
    public void onStart() {
      startCount++;
    }

    @Override
    public void onElement() {
      elementCount++;
    }

    @Override
    public void onEnd() {
      endCount++;
    }

    @Override
    public ValueListener objectArrayElement(int arrayDims) {
      return element(arrayDims, JsonType.OBJECT);
    }

    @Override
    public ValueListener objectElement() {
      return element(0, JsonType.OBJECT);
    }

    @Override
    public ValueListener arrayElement(int arrayDims, JsonType type) {
      return element(arrayDims, type);
    }

    @Override
    public ValueListener scalarElement(JsonType type) {
      return element(0, type);
    }

    private ValueListener element(int arrayDims, JsonType type) {
      assertNull(element);
      element = new ValueListenerFixture(arrayDims, type);
      return element;
    }
  }

  protected static class ObjectListenerFixture implements ObjectListener {

    final Map<String, ValueListenerFixture> fields = new HashMap<>();
    Set<String> projectFilter;
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
    public boolean isProjected(String key) {
      return projectFilter == null || projectFilter.contains(key);
    }

    @Override
    public ValueListener addScalar(String key, JsonType type) {
      return field(key, 0, type);
    }

    @Override
    public ValueListener addArray(String key, int dims, JsonType type) {
      return field(key, dims, type);
    }

    @Override
    public ValueListener addObject(String key) {
      return field(key, 0, JsonType.OBJECT);
    }

    @Override
    public ValueListener addObjectArray(String key, int dims) {
      return field(key, dims, JsonType.OBJECT);
    }

    private ValueListener field(String key, int dims, JsonType type) {
      assertFalse(fields.containsKey(key));
      ValueListenerFixture field = new ValueListenerFixture(dims, type);
      fields.put(key, field);
      return field;
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
