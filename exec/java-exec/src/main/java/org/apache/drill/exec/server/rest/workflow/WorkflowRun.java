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

import java.util.List;

/**
 * One recorded run, stored in drill.sqllab.snapshots. Field names stay compatible
 * with pre-workflow snapshots (scheduleId is the workflow id).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class WorkflowRun {
  public String id;
  public String scheduleId;
  public String savedQueryId;
  public String executedAt;
  public String status;
  public String trigger;
  public Long rowCount;
  public Long duration;
  public String errorMessage;
  public String previewRows;
  public String previewColumns;
  public Long previousRowCount;
  public Long rowCountDelta;
  public List<ActionOutcome> actions;
  // Legacy, read-only
  public String aiSummary;
  public String triggeredAlerts;
  public String resultPath;
}
