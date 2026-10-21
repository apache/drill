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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

/** Converts pre-workflow schedules (drill.sqllab.schedules) into workflows. */
public final class WorkflowMigration {
  private static final ObjectMapper M = new ObjectMapper();

  /** Shape of the old QueryScheduleModel; only fields the migration reads. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class LegacySchedule {
    public String id;
    public String savedQueryId;
    public String owner;
    public String description;
    public String frequency;
    public boolean enabled;
    public String timeOfDay;
    public Integer dayOfWeek;
    public Integer dayOfMonth;
    public boolean notifyOnSuccess;
    public boolean notifyOnFailure;
    public List<String> notifyEmails;
    public int retentionCount = 30;
    public String nextRunAt;
    public String lastRunAt;
    public String createdAt;
    public String expiresAt;
    public String renewedAt;
    public boolean persistResults;
    public String resultLocation;
    public String resultFormat;
    public String resultMode;
    public String alertRules;
    public int timeoutSeconds = 300;
    public boolean paused;
  }

  private WorkflowMigration() { }

  public static Workflow convert(LegacySchedule o, String projectId, String savedQueryName) {
    Workflow wf = new Workflow();
    wf.id = o.id;
    wf.projectId = projectId;
    wf.name = savedQueryName != null ? savedQueryName : (o.description != null ? o.description : "Workflow " + o.id);
    wf.description = o.description;
    // The old scheduler ran ownerless schedules as anonymous; record that so membership checks agree.
    wf.owner = o.owner == null || o.owner.isEmpty() ? DrillUserPrincipal.ANONYMOUS_USER : o.owner;
    wf.savedQueryId = o.savedQueryId;
    wf.schedule.frequency = o.frequency;
    wf.schedule.timeOfDay = o.timeOfDay != null ? o.timeOfDay : "08:00";
    // The validator requires the matching day field and accepts dayOfMonth only in 1-28.
    // Legacy used java.time.DayOfWeek numbering, where 7 is Sunday; workflows use 0 for Sunday.
    if (o.dayOfWeek != null && o.dayOfWeek == 7) {
      wf.schedule.dayOfWeek = 0;
    } else {
      wf.schedule.dayOfWeek = o.dayOfWeek == null && "weekly".equals(o.frequency) ? Integer.valueOf(1) : o.dayOfWeek;
    }
    if (o.dayOfMonth != null) {
      wf.schedule.dayOfMonth = Math.max(1, Math.min(28, o.dayOfMonth));
    } else if ("monthly".equals(o.frequency)) {
      wf.schedule.dayOfMonth = 1;
    }
    wf.schedule.timeoutSeconds = o.timeoutSeconds > 0 ? o.timeoutSeconds : 300;
    // Orphans stay off until an admin assigns them to a project.
    wf.schedule.enabled = o.enabled && !o.paused && projectId != null;
    wf.schedule.expiresAt = o.expiresAt;
    wf.retentionCount = o.retentionCount > 0 ? o.retentionCount : 30;
    wf.status = wf.schedule.enabled ? "active" : "paused";
    wf.createdAt = o.createdAt;
    wf.renewedAt = o.renewedAt;
    wf.lastRunAt = o.lastRunAt;
    // Recomputed rather than copied: legacy times were UTC, and a slot missed during the upgrade
    // must not fire (and email) on the first tick.
    wf.nextRunAt = WorkflowSchedule.nextRun(wf.schedule, Instant.now(), ZoneId.systemDefault()).toString();

    if (o.persistResults) {
      ObjectNode cfg = M.createObjectNode();
      cfg.put("location", o.resultLocation != null ? o.resultLocation : "dfs.tmp");
      cfg.put("format", o.resultFormat != null ? o.resultFormat : "parquet");
      cfg.put("mode", o.resultMode != null ? o.resultMode : "overwrite");
      wf.actions.add(action("save_results", "save_to_table", null, cfg));
    }
    addAlertActions(o, wf);
    if (o.notifyEmails != null && !o.notifyEmails.isEmpty()) {
      if (o.notifyOnFailure) {
        wf.actions.add(action("email_on_failure", "email", "status == 'error'",
            email(o.notifyEmails, "Workflow failed: " + wf.name, "{{error}}")));
      }
      if (o.notifyOnSuccess) {
        wf.actions.add(action("email_on_success", "email", "status == 'success'",
            email(o.notifyEmails, "Workflow succeeded: " + wf.name, "{{rowCount}} rows")));
      }
    }
    return wf;
  }

  private static void addAlertActions(LegacySchedule o, Workflow wf) {
    if (o.alertRules == null || o.alertRules.isEmpty()) {
      return;
    }
    JsonNode rules;
    try {
      rules = M.readTree(o.alertRules);
    } catch (Exception e) {
      return;
    }
    long halfInterval = WorkflowSchedule.intervalMillis(wf.schedule) / 2;
    for (JsonNode r : rules) {
      if (!r.path("enabled").asBoolean(false)) {
        continue;
      }
      long t = r.path("threshold").asLong(0);
      String expr;
      String message;
      switch (r.path("type").asText()) {
        case "rowCountAbove":
          expr = "rowCount > " + t;
          message = "Row count {{rowCount}} exceeds " + t;
          break;
        case "rowCountBelow":
          expr = "rowCount < " + t;
          message = "Row count {{rowCount}} is below " + t;
          break;
        case "queryFailed":
          expr = "status == 'error'";
          message = "Query failed: {{error}}";
          break;
        case "durationAbove":
          expr = "durationMs > " + (t * 1000);
          message = "Took {{durationMs}} ms";
          break;
        case "durationExceedsInterval":
          expr = "durationMs > " + halfInterval;
          message = "Took {{durationMs}} ms, over half the schedule interval";
          break;
        default:
          continue;
      }
      ObjectNode cfg = M.createObjectNode();
      cfg.put("message", message);
      wf.actions.add(action("alert_" + r.path("id").asText().replaceAll("[^A-Za-z0-9_]", "_"), "record_alert", expr, cfg));
    }
  }

  private static ObjectNode email(List<String> to, String subject, String body) {
    ObjectNode cfg = M.createObjectNode();
    ArrayNode arr = cfg.putArray("to");
    to.forEach(arr::add);
    cfg.put("subject", subject);
    cfg.put("body", body);
    return cfg;
  }

  private static Workflow.ActionSpec action(String id, String type, String expr, ObjectNode config) {
    Workflow.ActionSpec a = new Workflow.ActionSpec();
    a.id = id;
    a.type = type;
    a.config = config;
    if (expr != null) {
      try {
        a.condition = Workflow.Condition.tree(ConditionParser.parse(expr));
      } catch (ConditionParser.ParseException e) {
        throw new IllegalStateException("Bad generated expression: " + expr, e);
      }
    }
    return a;
  }
}
