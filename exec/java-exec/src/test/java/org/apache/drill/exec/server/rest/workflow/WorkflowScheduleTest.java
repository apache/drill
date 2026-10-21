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
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WorkflowScheduleTest {
  private static final ZoneId UTC = ZoneId.of("UTC");
  // Wednesday 2026-10-21 10:30 UTC
  private static final Instant NOW = Instant.parse("2026-10-21T10:30:00Z");

  private static Workflow.Schedule s(String freq, String time, Integer dow, Integer dom) {
    Workflow.Schedule s = new Workflow.Schedule();
    s.frequency = freq;
    s.timeOfDay = time;
    s.dayOfWeek = dow;
    s.dayOfMonth = dom;
    return s;
  }

  @Test
  public void hourly() {
    assertEquals(Instant.parse("2026-10-21T10:45:00Z"), WorkflowSchedule.nextRun(s("hourly", "00:45", null, null), NOW, UTC));
    assertEquals(Instant.parse("2026-10-21T11:15:00Z"), WorkflowSchedule.nextRun(s("hourly", "00:15", null, null), NOW, UTC));
  }

  @Test
  public void dailyTodayOrTomorrow() {
    assertEquals(Instant.parse("2026-10-21T14:00:00Z"), WorkflowSchedule.nextRun(s("daily", "14:00", null, null), NOW, UTC));
    assertEquals(Instant.parse("2026-10-22T08:00:00Z"), WorkflowSchedule.nextRun(s("daily", "08:00", null, null), NOW, UTC));
    assertEquals(Instant.parse("2026-10-22T10:30:00Z"), WorkflowSchedule.nextRun(s("daily", "10:30", null, null), NOW, UTC));
  }

  @Test
  public void weeklyUsesSundayZero() {
    assertEquals(Instant.parse("2026-10-25T09:00:00Z"), WorkflowSchedule.nextRun(s("weekly", "09:00", 0, null), NOW, UTC));
    assertEquals(Instant.parse("2026-10-28T09:00:00Z"), WorkflowSchedule.nextRun(s("weekly", "09:00", 3, null), NOW, UTC));
    assertEquals(Instant.parse("2026-10-21T11:00:00Z"), WorkflowSchedule.nextRun(s("weekly", "11:00", 3, null), NOW, UTC));
  }

  @Test
  public void monthly() {
    assertEquals(Instant.parse("2026-11-01T08:00:00Z"), WorkflowSchedule.nextRun(s("monthly", "08:00", null, 1), NOW, UTC));
    assertEquals(Instant.parse("2026-10-28T08:00:00Z"), WorkflowSchedule.nextRun(s("monthly", "08:00", null, 28), NOW, UTC));
  }

  @Test
  public void expiry() {
    WorkflowConfig cfg = new WorkflowConfig();
    Workflow wf = new Workflow();
    wf.schedule.expiresAt = "2026-10-20T00:00:00Z";
    assertTrue(WorkflowSchedule.isExpired(wf, NOW, cfg));
    cfg.expirationEnabled = false;
    assertFalse(WorkflowSchedule.isExpired(wf, NOW, cfg));
    cfg.expirationEnabled = true;
    wf.schedule.expiresAt = null;
    assertFalse(WorkflowSchedule.isExpired(wf, NOW, cfg));
  }
}
