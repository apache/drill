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

import org.apache.drill.exec.server.rest.ProjectResources;
import org.apache.drill.exec.server.rest.SavedQueryResources;
import org.apache.drill.exec.server.rest.smtp.SmtpConfig;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.work.WorkManager;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Lazily opened persistent stores used by the runner and the REST resource. */
public class WorkflowStores {
  static final String WORKFLOWS = "drill.sqllab.workflows";
  static final String RUNS = "drill.sqllab.snapshots";
  static final String CLAIMS = "drill.sqllab.workflow_claims";
  static final String LEGACY_SCHEDULES = "drill.sqllab.schedules";
  static final String SAVED_QUERIES = "drill.sqllab.saved_queries";
  static final String CONFIG = "drill.sqllab.workflow_config";
  static final String SMTP = "drill.sqllab.smtp_config";
  static final String DEFAULT_KEY = "default";

  private final PersistentStoreProvider provider;
  private final WorkManager workManager;
  private final Map<String, PersistentStore<?>> open = new ConcurrentHashMap<>();

  public WorkflowStores(PersistentStoreProvider provider, WorkManager workManager) {
    this.provider = provider;
    this.workManager = workManager;
  }

  @SuppressWarnings("unchecked")
  private <V> PersistentStore<V> store(String name, Class<V> type) {
    return (PersistentStore<V>) open.computeIfAbsent(name, n -> {
      try {
        return provider.getOrCreateStore(PersistentStoreConfig.newJacksonBuilder(
            workManager.getContext().getLpPersistence().getMapper(), type).name(n).build());
      } catch (Exception e) {
        throw new IllegalStateException("Cannot open store " + n, e);
      }
    });
  }

  public PersistentStore<Workflow> workflows() {
    return store(WORKFLOWS, Workflow.class);
  }

  public PersistentStore<WorkflowRun> runs() {
    return store(RUNS, WorkflowRun.class);
  }

  public PersistentStore<RunClaims.Claim> claims() {
    return store(CLAIMS, RunClaims.Claim.class);
  }

  public PersistentStore<WorkflowMigration.LegacySchedule> legacySchedules() {
    return store(LEGACY_SCHEDULES, WorkflowMigration.LegacySchedule.class);
  }

  public PersistentStore<SavedQueryResources.SavedQuery> savedQueries() {
    return store(SAVED_QUERIES, SavedQueryResources.SavedQuery.class);
  }

  public PersistentStore<ProjectResources.Project> projects() {
    return ProjectResources.openStore(provider, workManager);
  }

  public PersistentStore<WorkflowConfig> config() {
    return store(CONFIG, WorkflowConfig.class);
  }

  public PersistentStore<SmtpConfig> smtp() {
    return store(SMTP, SmtpConfig.class);
  }

  public WorkflowConfig loadConfig() {
    WorkflowConfig c = config().get(DEFAULT_KEY);
    return c != null ? c : new WorkflowConfig();
  }

  public void saveConfig(WorkflowConfig c) {
    config().put(DEFAULT_KEY, c);
  }

  public SavedQueryResources.SavedQuery savedQuery(String id) {
    SavedQueryResources.SavedQuery q = id == null ? null : savedQueries().get(id);
    return q == null || q.getDeletedAt() > 0 ? null : q;
  }

  public ProjectResources.Project project(String id) {
    ProjectResources.Project p = id == null ? null : projects().get(id);
    return p == null || p.getDeletedAt() > 0 ? null : p;
  }

  /** ponytail: full scan of projects; fine for hundreds of projects, index if that grows. */
  public String projectForQuery(String savedQueryId) {
    Iterator<Map.Entry<String, ProjectResources.Project>> it = projects().getAll();
    while (it.hasNext()) {
      ProjectResources.Project p = it.next().getValue();
      if (p != null && p.getDeletedAt() == 0 && p.getSavedQueryIds().contains(savedQueryId)) {
        return p.getId();
      }
    }
    return null;
  }
}
