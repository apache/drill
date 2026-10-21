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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

public class RunContextTest {

  private RunContext ctx() {
    RunContext c = new RunContext();
    c.status = "success";
    c.rowCount = 120;
    c.durationMs = 900;
    c.rows = List.of(Map.of("user", "root", "n", "3"), Map.of("user", "bob", "n", "7"));
    c.actionOutputs.put("judge", Map.of("verdict", true, "reason", "spike"));
    return c;
  }

  @Test
  public void resolvesBaseFields() {
    assertEquals("success", ctx().resolve("status"));
    assertEquals(120L, ctx().resolve("rowCount"));
    assertNull(ctx().resolve("rowCountDelta"));
  }

  @Test
  public void resolvesRowColumnAsList() {
    assertEquals(List.of("root", "bob"), ctx().resolve("rows[*].user"));
  }

  @Test
  public void resolvesActionOutputs() {
    assertEquals(true, ctx().resolve("actions.judge.verdict"));
    assertSame(RunContext.MISSING, ctx().resolve("actions.judge.nope"));
    assertSame(RunContext.MISSING, ctx().resolve("actions.other.verdict"));
  }

  @Test
  public void unknownPathIsMissing() {
    assertSame(RunContext.MISSING, ctx().resolve("rowcount"));
  }
}
