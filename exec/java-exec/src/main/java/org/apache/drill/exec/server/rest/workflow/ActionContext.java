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

/** Everything an action needs while executing. */
public class ActionContext {
  private final Workflow workflow;
  private final String sql;
  private final RunContext run;
  private final WorkflowServices services;

  public ActionContext(Workflow workflow, String sql, RunContext run, WorkflowServices services) {
    this.workflow = workflow;
    this.sql = sql;
    this.run = run;
    this.services = services;
  }

  public Workflow workflow() { return workflow; }

  public String sql() { return sql; }

  public RunContext run() { return run; }

  public WorkflowServices services() { return services; }

  public String render(String template) {
    return Templates.render(template, run);
  }
}
