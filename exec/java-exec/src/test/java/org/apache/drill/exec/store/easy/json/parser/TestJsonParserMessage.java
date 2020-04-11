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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.drill.exec.store.easy.json.parser.ValueDef.JsonType;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonToken;

public class TestJsonParserMessage extends BaseTestJsonParser {

  /**
   * Example message parser. A real parser would provide much better
   * error messages for badly-formed JSON or error codes.
   */
  private static class MessageParserFixture implements MessageParser {

    @Override
    public boolean parsePrefix(TokenIterator tokenizer) {
      assertEquals(JsonToken.START_OBJECT, tokenizer.requireNext());
      assertEquals(JsonToken.FIELD_NAME, tokenizer.requireNext());
      assertEquals(JsonToken.VALUE_STRING, tokenizer.requireNext());
      if (!"ok".equals(tokenizer.stringValue())) {
        return false;
      }
      assertEquals(JsonToken.FIELD_NAME, tokenizer.requireNext());
      assertEquals(JsonToken.START_ARRAY, tokenizer.requireNext());
      return true;
    }

    @Override
    public void parseSuffix(TokenIterator tokenizer) {
      assertEquals(JsonToken.END_OBJECT, tokenizer.requireNext());
    }
  }

  /**
   * Test the ability to wrap the data objects with a custom message
   * structure, typical of a REST call.
   */
  @Test
  public void testMessageParser() {
    final String json =
        "{ status: \"ok\", data: [{a: 0}, {a: 100}, {a: null}]}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.builder.messageParser(new MessageParserFixture());
    fixture.open(json);
    assertTrue(fixture.next());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(JsonType.INTEGER, a.valueDef.type());
    assertEquals(2, fixture.read());
    assertEquals(1, a.nullCount);
    assertEquals(100L, a.value);
    fixture.close();
  }

  /**
   * Test the ability to cancel the data load if a message header
   * indicates that there is no data.
   */
  @Test
  public void testMessageParserEOF() {
    final String json =
        "{ status: \"fail\", data: [{a: 0}, {a: 100}, {a: null}]}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.builder.messageParser(new MessageParserFixture());
    fixture.open(json);
    assertFalse(fixture.next());
    fixture.close();
  }

  @Test
  public void testDataPath() {
    final String json =
        "{ status: \"ok\", data: [{a: 0}, {a: 100}, {a: null}]}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.builder.dataPath("data");
    fixture.open(json);
    assertTrue(fixture.next());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(JsonType.INTEGER, a.valueDef.type());
    assertEquals(2, fixture.read());
    assertEquals(1, a.nullCount);
    assertEquals(100L, a.value);
    fixture.close();
  }

  @Test
  public void testComplexDataPath() {
    final String json =
        "{ status: {result : \"ok\", runtime: 123},\n" +
        "  response: { rowCount: 1,\n" +
        "    data: [{a: 0}, {a: 100}, {a: null}]},\n" +
        "  footer: \"some stuff\"}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.builder.dataPath("response/data");
    fixture.open(json);
    assertTrue(fixture.next());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(JsonType.INTEGER, a.valueDef.type());
    assertEquals(2, fixture.read());
    assertEquals(1, a.nullCount);
    assertEquals(100L, a.value);
    fixture.close();
  }

  @Test
  public void testDataPathNull() {
    final String json =
        "{ status: \"fail\", data: null}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.builder.messageParser(new MessageParserFixture());
    fixture.open(json);
    assertFalse(fixture.next());
    fixture.close();
  }

  @Test
  public void testDataPathMissing() {
    final String json =
        "{ status: \"fail\"}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.builder.messageParser(new MessageParserFixture());
    fixture.open(json);
    assertFalse(fixture.next());
    fixture.close();
  }

  @Test
  public void testDataPathErrorRoot() {
    final String json = "\"Bogus!\"";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.builder.dataPath("data");
    try {
      fixture.open(json);
      fail();
    } catch (JsonErrorFixture e) {
      assertTrue(e.errorType.equals("messageParseError"));
      assertTrue(e.getCause() instanceof MessageParser.MessageContextException);
    }
    fixture.close();
  }

  @Test
  public void testDataPathErrorLeaf() {
    final String json =
        "{ status: \"bogus\", data: { notValid: \"must be array\"}}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.builder.dataPath("data");
    try {
      fixture.open(json);
      fail();
    } catch (JsonErrorFixture e) {
      assertTrue(e.errorType.equals("messageParseError"));
      assertTrue(e.getCause() instanceof MessageParser.MessageContextException);
    }
    fixture.close();
  }
}
