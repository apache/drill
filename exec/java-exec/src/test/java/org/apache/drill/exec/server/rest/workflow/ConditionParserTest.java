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
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConditionParserTest {
  private static final ObjectMapper M = new ObjectMapper();

  private static JsonNode json(String s) throws Exception {
    return M.readTree(s.replace('\'', '"'));
  }

  @Test
  public void simpleComparison() throws Exception {
    assertEquals(json("{'field':'rowCount','op':'>','value':50}"),
        ConditionParser.parse("rowCount > 50"));
  }

  @Test
  public void andBindsTighterThanOr() throws Exception {
    assertEquals(json("{'any':[{'field':'a','op':'>','value':1},"
            + "{'all':[{'field':'b','op':'<','value':2},{'field':'c','op':'==','value':'x'}]}]}"),
        ConditionParser.parse("a > 1 || b < 2 && c == 'x'"));
  }

  @Test
  public void parensNotAndBarePath() throws Exception {
    assertEquals(json("{'all':[{'not':{'any':[{'field':'a','op':'==','value':true},"
            + "{'field':'b','op':'!=','value':null}]}},{'field':'actions.judge.verdict','op':'==','value':true}]}"),
        ConditionParser.parse("!(a || b != null) && actions.judge.verdict"));
  }

  @Test
  public void rowPathsWordOpsAndStrings() throws Exception {
    assertEquals(json("{'all':[{'field':'rows[*].user','op':'startsWith','value':'ro\\u0027o'},"
            + "{'field':'error','op':'contains','value':'timeout'}]}"),
        ConditionParser.parse("rows[*].user startsWith \"ro'o\" && error contains 'timeout'"));
  }

  @Test
  public void negativeAndDecimalNumbers() throws Exception {
    assertEquals(json("{'field':'deltaPercent','op':'<=','value':-12.5}"),
        ConditionParser.parse("deltaPercent <= -12.5"));
  }

  @Test
  public void errorsCarryPosition() {
    ConditionParser.ParseException e1 = assertThrows(ConditionParser.ParseException.class,
        () -> ConditionParser.parse("rowCount >"));
    assertEquals(10, e1.position);
    ConditionParser.ParseException e2 = assertThrows(ConditionParser.ParseException.class,
        () -> ConditionParser.parse("rowCount > 5 )"));
    assertEquals(13, e2.position);
    ConditionParser.ParseException e3 = assertThrows(ConditionParser.ParseException.class,
        () -> ConditionParser.parse("5 > rowCount"));
    assertEquals(0, e3.position);
    assertThrows(ConditionParser.ParseException.class, () -> ConditionParser.parse("   "));
    assertThrows(ConditionParser.ParseException.class, () -> ConditionParser.parse("a == 'open"));
  }

  @Test
  public void nestingDepthIsLimited() throws Exception {
    String ok = "(".repeat(50) + "a" + ")".repeat(50);
    ConditionParser.parse(ok);
    String deep = "(".repeat(51) + "a" + ")".repeat(51);
    ConditionParser.ParseException e = assertThrows(ConditionParser.ParseException.class,
        () -> ConditionParser.parse(deep));
    assertEquals(true, e.getMessage().contains("nested too deeply"));
    String nots = "!".repeat(51) + "a";
    assertThrows(ConditionParser.ParseException.class, () -> ConditionParser.parse(nots));
  }
}
