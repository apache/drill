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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConditionEvaluatorTest {

  private static RunContext ctx() {
    RunContext c = new RunContext();
    c.status = "success";
    c.rowCount = 120;
    c.rows = List.of(Map.of("user", "root", "failed", "3"), Map.of("user", "bob", "failed", "70"));
    c.actionOutputs.put("judge", Map.of("verdict", true));
    return c;
  }

  private static boolean eval(String expr) throws Exception {
    return ConditionEvaluator.evaluate(ConditionParser.parse(expr), ctx());
  }

  @Test
  public void numbersAndStrings() throws Exception {
    assertTrue(eval("rowCount > 100"));
    assertFalse(eval("rowCount >= 121"));
    assertTrue(eval("status == 'success'"));
    assertTrue(eval("status != 'error'"));
  }

  @Test
  public void rowColumnsMatchAnyRowAndCoerceNumbers() throws Exception {
    assertTrue(eval("rows[*].failed > 50"));
    assertFalse(eval("rows[*].failed > 100"));
    assertTrue(eval("rows[*].user startsWith 'ro'"));
  }

  @Test
  public void booleansNullsAndMissing() throws Exception {
    assertTrue(eval("actions.judge.verdict"));
    assertTrue(eval("rowCountDelta == null"));
    assertFalse(eval("rowCountDelta > 0"));
    assertFalse(eval("actions.missing.verdict"));
    assertTrue(eval("error == null && !(status == 'error')"));
  }

  @Test
  public void alwaysAndNullConditions() {
    assertTrue(ConditionEvaluator.evaluate((Workflow.Condition) null, ctx()));
    assertTrue(ConditionEvaluator.evaluate(Workflow.Condition.always(), ctx()));
  }

  @Test
  public void unknownOperatorThrows() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> ConditionEvaluator.evaluate(
        new com.fasterxml.jackson.databind.ObjectMapper().readTree(
            "{\"field\":\"rowCount\",\"op\":\"~\",\"value\":1}"), ctx()));
  }
}
