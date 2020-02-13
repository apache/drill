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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

/**
 * Tests the un-happy path cases in the JSON structure parser. Some
 * error cases can't occur because the Jackson parser catches them
 * first.
 */
public class TestJsonParserErrors extends BaseTestJsonParser {

  @Test
  public void testMissingEndObject() {
    expectError("{a: 0} {a: 100", "syntaxError");
  }

  @Test
  public void testMissingValue() {
    expectError("{a: 0} {a: ", "syntaxError");
  }

  /**
   * When parsing an array, the Jackson JSON parser raises
   * an error for a missing close bracket.
   */
  @Test
  public void testMissingEndOuterArray() {
    expectError("[{a: 0}, {a: 100}", "syntaxError");
  }

  @Test
  public void testEmptyKey() {
    expectError("{\"\": 10}", "structureError");
  }

  @Test
  public void testBlankKey() {
    expectError("{\"  \": 10}", "structureError");
  }

  @Test
  public void testRootArrayDisallowed() {
    final String json = "[{a: 0}, {a: 100}, {a: null}]";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.options.skipOuterList = false;
    try {
      fixture.open(json);
      fail();
    } catch (JsonErrorFixture e) {
      assertEquals("structureError", e.errorType);
      assertTrue(e.getMessage().contains("includes an outer array"));
    }
    fixture.close();
  }

  /**
   * Test syntax error recover. Recovery is not perfect. The
   * input contains six records: the second is bad. But, the parser
   * consumes records 3 and 4 trying to recover.
   */
  @Test
  public void testRecovery() {
    final String json = "{a: 1} {a: {a: 3} {a: 4} {a: 5} {a: 6}";
    JsonParserFixture fixture = new JsonParserFixture();
    fixture.options.skipMalformedRecords = true;
    fixture.open(json);
    assertEquals(3, fixture.read());
    assertEquals(1, fixture.parser.recoverableErrorCount());
    ValueListenerFixture a = fixture.field("a");
    assertEquals(3, a.valueCount);
    fixture.close();
  }
}
