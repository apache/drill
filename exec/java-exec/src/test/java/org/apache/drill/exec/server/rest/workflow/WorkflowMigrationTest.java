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

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WorkflowMigrationTest {

  private static WorkflowMigration.LegacySchedule legacy() {
    WorkflowMigration.LegacySchedule o = new WorkflowMigration.LegacySchedule();
    o.id = "s-1";
    o.savedQueryId = "q-1";
    o.owner = "alice";
    o.frequency = "weekly";
    o.timeOfDay = "09:15";
    o.dayOfWeek = 2;
    o.enabled = true;
    o.expiresAt = "2027-01-01T00:00:00Z";
    o.timeoutSeconds = 120;
    o.retentionCount = 10;
    return o;
  }

  @Test
  public void scheduleFieldsCarryOver() {
    Workflow wf = WorkflowMigration.convert(legacy(), "p-1", "Failed logins");
    assertEquals("s-1", wf.id);
    assertEquals("p-1", wf.projectId);
    assertEquals("Failed logins", wf.name);
    assertEquals("alice", wf.owner);
    assertEquals("weekly", wf.schedule.frequency);
    assertEquals(2, wf.schedule.dayOfWeek);
    assertEquals(120, wf.schedule.timeoutSeconds);
    assertTrue(wf.schedule.enabled);
    assertEquals(10, wf.retentionCount);
    assertTrue(wf.actions.isEmpty());
  }

  @Test
  public void legacySundayBecomesZero() {
    WorkflowMigration.LegacySchedule o = legacy();
    o.dayOfWeek = 7;
    assertEquals(0, WorkflowMigration.convert(o, "p-1", "x").schedule.dayOfWeek);
  }

  @Test
  public void nextRunIsRecomputedSoMissedSlotsDoNotFireOnUpgrade() {
    WorkflowMigration.LegacySchedule o = legacy();
    o.nextRunAt = "2020-01-01T00:00:00Z";
    Instant before = Instant.now();
    Workflow wf = WorkflowMigration.convert(o, "p-1", "x");
    assertTrue(Instant.parse(wf.nextRunAt).isAfter(before));
    o.nextRunAt = "";
    assertTrue(Instant.parse(WorkflowMigration.convert(o, "p-1", "x").nextRunAt).isAfter(before));
  }

  @Test
  public void orphanIsDisabled() {
    Workflow wf = WorkflowMigration.convert(legacy(), null, "x");
    assertNull(wf.projectId);
    assertFalse(wf.schedule.enabled);
  }

  @Test
  public void dayOfMonthIsClampedTo28() {
    WorkflowMigration.LegacySchedule o = legacy();
    o.frequency = "monthly";
    o.dayOfMonth = 31;
    assertEquals(28, WorkflowMigration.convert(o, "p-1", "x").schedule.dayOfMonth);
  }

  @Test
  public void missingDaysDefaultSoValidatorAccepts() {
    WorkflowMigration.LegacySchedule o = legacy();
    o.dayOfWeek = null;
    assertEquals(1, WorkflowMigration.convert(o, "p-1", "x").schedule.dayOfWeek);
    o.frequency = "monthly";
    o.dayOfMonth = null;
    assertEquals(1, WorkflowMigration.convert(o, "p-1", "x").schedule.dayOfMonth);
  }

  @Test
  public void persistAlertsAndEmailsBecomeActionsInOrder() throws Exception {
    WorkflowMigration.LegacySchedule o = legacy();
    o.persistResults = true;
    o.resultLocation = "dfs.tmp";
    o.resultFormat = "json";
    o.resultMode = "append";
    o.alertRules = "[{\"id\":\"r1\",\"type\":\"rowCountAbove\",\"threshold\":50,\"enabled\":true},"
        + "{\"id\":\"r2\",\"type\":\"durationAbove\",\"threshold\":30,\"enabled\":true},"
        + "{\"id\":\"r3\",\"type\":\"queryFailed\",\"enabled\":false},"
        + "{\"id\":\"r4\",\"type\":\"durationExceedsInterval\",\"enabled\":true}]";
    o.notifyOnFailure = true;
    o.notifyOnSuccess = true;
    o.notifyEmails = List.of("ops@example.com");

    Workflow wf = WorkflowMigration.convert(o, "p-1", "Q");
    List<String> ids = wf.actions.stream().map(a -> a.id).collect(java.util.stream.Collectors.toList());
    assertEquals(List.of("save_results", "alert_r1", "alert_r2", "alert_r4", "email_on_failure", "email_on_success"), ids);
    assertEquals("{\"location\":\"dfs.tmp\",\"format\":\"json\",\"mode\":\"append\"}", wf.actions.get(0).config.toString());
    assertEquals(ConditionParser.parse("rowCount > 50"), wf.actions.get(1).condition.tree);
    assertEquals(ConditionParser.parse("durationMs > 30000"), wf.actions.get(2).condition.tree);
    assertEquals(ConditionParser.parse("durationMs > 302400000"), wf.actions.get(3).condition.tree);
    assertEquals(ConditionParser.parse("status == 'error'"), wf.actions.get(4).condition.tree);
    assertEquals("ops@example.com", wf.actions.get(5).config.get("to").get(0).asText());

    assertTrue(WorkflowValidator.validate(wf, ActionTypeRegistry.defaults()).isEmpty());
  }
}
