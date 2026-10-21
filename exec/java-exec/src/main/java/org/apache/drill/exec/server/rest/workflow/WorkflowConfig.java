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

/** Global workflow settings, stored in drill.sqllab.workflow_config under key "default". */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkflowConfig {
  public boolean expirationEnabled = true;
  public int expirationDays = 90;
  public int warningDaysBeforeExpiry = 14;
  public int actionTimeoutSeconds = 60;
  /** Set once legacy schedules have been converted; prevents re-creating deleted workflows. */
  public boolean legacyMigrated;
}
