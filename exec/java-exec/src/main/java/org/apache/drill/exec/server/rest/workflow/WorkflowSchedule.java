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

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAdjusters;

/** Schedule arithmetic. Day-of-week is 0 = Sunday ... 6 = Saturday. */
public final class WorkflowSchedule {

  private WorkflowSchedule() { }

  public static Instant nextRun(Workflow.Schedule s, Instant from, ZoneId zone) {
    LocalTime t = parseTime(s.timeOfDay);
    ZonedDateTime now = from.atZone(zone);
    ZonedDateTime c;
    switch (s.frequency == null ? "daily" : s.frequency) {
      case "hourly":
        c = now.withMinute(t.getMinute()).withSecond(0).withNano(0);
        if (!c.isAfter(now)) {
          c = c.plusHours(1);
        }
        break;
      case "weekly":
        c = now.with(TemporalAdjusters.nextOrSame(dayOfWeek(s.dayOfWeek == null ? 1 : s.dayOfWeek))).with(t);
        if (!c.isAfter(now)) {
          c = c.plusWeeks(1);
        }
        break;
      case "monthly":
        int dom = s.dayOfMonth == null ? 1 : Math.max(1, Math.min(28, s.dayOfMonth));
        c = now.withDayOfMonth(dom).with(t);
        if (!c.isAfter(now)) {
          c = c.plusMonths(1);
        }
        break;
      default:
        c = now.with(t);
        if (!c.isAfter(now)) {
          c = c.plusDays(1);
        }
        break;
    }
    return c.toInstant();
  }

  public static long intervalMillis(Workflow.Schedule s) {
    switch (s.frequency == null ? "daily" : s.frequency) {
      case "hourly": return 3_600_000L;
      case "weekly": return 604_800_000L;
      case "monthly": return 2_592_000_000L;
      default: return 86_400_000L;
    }
  }

  public static boolean isExpired(Workflow wf, Instant now, WorkflowConfig cfg) {
    if (!cfg.expirationEnabled || wf.schedule.expiresAt == null || wf.schedule.expiresAt.isEmpty()) {
      return false;
    }
    return Instant.parse(wf.schedule.expiresAt).isBefore(now);
  }

  static DayOfWeek dayOfWeek(int d) {
    return d == 0 ? DayOfWeek.SUNDAY : DayOfWeek.of(d);
  }

  private static LocalTime parseTime(String hhmm) {
    try {
      return LocalTime.parse(hhmm == null ? "08:00" : hhmm);
    } catch (RuntimeException e) {
      return LocalTime.of(8, 0);
    }
  }
}
