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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

/**
 * A workflow: a saved query on a schedule followed by an ordered list of actions.
 * This one document is shared by the REST API, the UI and Prospector.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Workflow {
  public String id;
  public String projectId;
  public String name;
  public String description;
  public String owner;
  public String savedQueryId;
  public Schedule schedule = new Schedule();
  public List<ActionSpec> actions = new ArrayList<>();
  public int retentionCount = 30;
  /** active | paused | expired | owner_lost_access */
  public String status = "active";
  public String createdAt;
  public String renewedAt;
  public String lastRunAt;
  public String nextRunAt;
  /** success | error of the most recent run */
  public String lastStatus;
  public int consecutiveFailures;
  public boolean isRunning;
  /** When the current run started; lets the runner tell a live run from one a crashed Drillbit left behind. */
  public String runStartedAt;

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class Schedule {
    /** hourly | daily | weekly | monthly */
    public String frequency = "daily";
    /** HH:mm; for hourly only the minutes matter */
    public String timeOfDay = "08:00";
    /** 0 = Sunday ... 6 = Saturday (weekly only) */
    public Integer dayOfWeek;
    /** 1-28 (monthly only) */
    public Integer dayOfMonth;
    public int timeoutSeconds = 300;
    public boolean enabled;
    public String expiresAt;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ActionSpec {
    public String id;
    public String type;
    public Condition condition = Condition.always();
    public ObjectNode config;
  }

  /** Exactly one of always / tree / expr is meaningful; expr is parsed into tree on save. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class Condition {
    public Boolean always;
    public JsonNode tree;
    public String expr;

    public static Condition always() {
      Condition c = new Condition();
      c.always = true;
      return c;
    }

    public static Condition tree(JsonNode tree) {
      Condition c = new Condition();
      c.tree = tree;
      return c;
    }
  }
}
