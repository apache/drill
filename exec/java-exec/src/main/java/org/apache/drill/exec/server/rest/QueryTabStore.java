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
package org.apache.drill.exec.server.rest;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.work.WorkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Per-user storage for SQL Lab query tabs, in the {@code drill.sqllab.tabs}
 * PersistentStore.
 *
 * <p>A tab reaches this store only once it has been promoted — a query has been run
 * from it, or it was closed while holding content. Unpromoted tabs live in the
 * browser's localStorage and never arrive here. See {@code docs/dev/TabPersistence.md}.
 *
 * <p>Results are deliberately not stored. A tab keeps only its {@code cacheId}; the
 * rows themselves live in the results cache under its own TTL, so a tab reopened long
 * after it was hidden comes back with its SQL and an expired result set.
 */
public class QueryTabStore {

  private static final Logger logger = LoggerFactory.getLogger(QueryTabStore.class);

  private static final String STORE_NAME = "drill.sqllab.tabs";

  /** Stands in for a null project id, for tabs opened outside a project at /query. */
  private static final String GLOBAL_PROJECT = "_global";

  private static volatile PersistentStore<TabRecord> cachedStore;

  // Memoized so all callers share one instance, mirroring ProjectSchemaCache.
  private static volatile QueryTabStore instance;

  private final PersistentStore<TabRecord> store;

  public QueryTabStore(PersistentStore<TabRecord> store) {
    this.store = store;
  }

  public static QueryTabStore get(PersistentStoreProvider provider, WorkManager workManager) {
    if (instance == null) {
      synchronized (QueryTabStore.class) {
        if (instance == null) {
          try {
            cachedStore = provider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    TabRecord.class)
                    .name(STORE_NAME)
                    .build());
          } catch (StoreException e) {
            throw new DrillRuntimeException("Failed to access query tab store", e);
          }
          instance = new QueryTabStore(cachedStore);
        }
      }
    }
    return instance;
  }

  /**
   * The store key is the tab id alone.
   *
   * <p>Under {@code LocalPersistentStoreProvider} this key becomes a filename, so it
   * cannot carry arbitrary user-supplied text. A previous version joined owner, project
   * and tab id with a NUL separator to isolate namespaces; NUL cannot appear in a path
   * and every write failed with "Invalid file path" outside ZooKeeper deployments.
   *
   * <p>Nothing is lost. Tab ids are UUIDs, so keys do not collide across users or
   * projects, and isolation comes from the owner and project fields on the record that
   * {@link #list} filters on — plus the ownership checks in {@code QueryTabResources},
   * which is where it belongs.
   *
   * <p>The owner and projectId parameters are kept so callers read naturally and so a
   * future key scheme has them available.
   */
  static String storeKey(String owner, String projectId, String tabId) {
    return tabId;
  }

  /**
   * Every tab belonging to one user in one project. Pass a null {@code projectId} for
   * the global tabs at /query.
   *
   * <p>{@link PersistentStore} has no prefix scan, so this filters {@code getAll()},
   * the same way {@code ProjectResources} lists projects. Tab counts per user are
   * small.
   */
  public List<TabRecord> list(String owner, String projectId) {
    String project = projectId == null || projectId.isEmpty() ? GLOBAL_PROJECT : projectId;
    List<TabRecord> found = new ArrayList<>();
    Iterator<Map.Entry<String, TabRecord>> entries = store.getAll();
    while (entries.hasNext()) {
      TabRecord record = entries.next().getValue();
      if (record == null) {
        continue;
      }
      String recordProject = record.getProjectId() == null || record.getProjectId().isEmpty()
          ? GLOBAL_PROJECT : record.getProjectId();
      if (Objects.equals(owner, record.getOwner()) && project.equals(recordProject)) {
        found.add(record);
      }
    }
    return found;
  }

  /**
   * Looks a tab up by id. Callers must still check ownership before acting on the
   * result — this returns any user's tab.
   */
  public TabRecord find(String id) {
    Iterator<Map.Entry<String, TabRecord>> entries = store.getAll();
    while (entries.hasNext()) {
      TabRecord record = entries.next().getValue();
      if (record != null && id.equals(record.getId())) {
        return record;
      }
    }
    return null;
  }

  public void save(TabRecord record) {
    store.put(storeKey(record.getOwner(), record.getProjectId(), record.getId()), record);
  }

  public void delete(String id) {
    TabRecord record = find(id);
    if (record == null) {
      logger.debug("No tab to delete for id {}", id);
      return;
    }
    store.delete(storeKey(record.getOwner(), record.getProjectId(), record.getId()));
  }

  /**
   * One query tab. {@code hidden} is what makes closing non-destructive: a hidden tab
   * leaves the tab strip but stays listed in the project tree.
   */
  public static class TabRecord {

    @JsonProperty
    private String id;

    /** Null for tabs opened outside a project, at /query. */
    @JsonProperty
    private String projectId;

    /** Always set server-side from the authenticated principal, never from the body. */
    @JsonProperty
    private String owner;

    @JsonProperty
    private String name;

    @JsonProperty
    private String sql;

    @JsonProperty
    private String defaultSchema;

    @JsonProperty
    private boolean hidden;

    @JsonProperty
    private long createdAt;

    @JsonProperty
    private long updatedAt;

    /** Visualizations created from this tab; used to warn before deletion. */
    @JsonProperty
    private List<String> vizIds;

    /** Locked tabs may be hidden but never deleted. */
    @JsonProperty
    private boolean locked;

    @JsonProperty
    private String lockReason;

    /** "manual" or "api". */
    @JsonProperty
    private String lockType;

    @JsonProperty
    private boolean pinned;

    /** Handle into the results cache. The rows themselves are not stored here. */
    @JsonProperty
    private String cacheId;

    public TabRecord() {
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getProjectId() {
      return projectId;
    }

    public void setProjectId(String projectId) {
      this.projectId = projectId;
    }

    public String getOwner() {
      return owner;
    }

    public void setOwner(String owner) {
      this.owner = owner;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getSql() {
      return sql;
    }

    public void setSql(String sql) {
      this.sql = sql;
    }

    public String getDefaultSchema() {
      return defaultSchema;
    }

    public void setDefaultSchema(String defaultSchema) {
      this.defaultSchema = defaultSchema;
    }

    public boolean isHidden() {
      return hidden;
    }

    public void setHidden(boolean hidden) {
      this.hidden = hidden;
    }

    public long getCreatedAt() {
      return createdAt;
    }

    public void setCreatedAt(long createdAt) {
      this.createdAt = createdAt;
    }

    public long getUpdatedAt() {
      return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
      this.updatedAt = updatedAt;
    }

    public List<String> getVizIds() {
      return vizIds;
    }

    public void setVizIds(List<String> vizIds) {
      this.vizIds = vizIds;
    }

    public boolean isLocked() {
      return locked;
    }

    public void setLocked(boolean locked) {
      this.locked = locked;
    }

    public String getLockReason() {
      return lockReason;
    }

    public void setLockReason(String lockReason) {
      this.lockReason = lockReason;
    }

    public String getLockType() {
      return lockType;
    }

    public void setLockType(String lockType) {
      this.lockType = lockType;
    }

    public boolean isPinned() {
      return pinned;
    }

    public void setPinned(boolean pinned) {
      this.pinned = pinned;
    }

    public String getCacheId() {
      return cacheId;
    }

    public void setCacheId(String cacheId) {
      this.cacheId = cacheId;
    }
  }
}
