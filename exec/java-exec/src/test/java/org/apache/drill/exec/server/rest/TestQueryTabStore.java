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

import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.work.WorkManager;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Storage-level tests for {@link QueryTabStore}.
 *
 * <p>Tabs are per-user scratch documents, so the scoping assertions here are the
 * security boundary: one user must never see or reach another user's tabs, and a
 * project's tabs must not leak into another project's listing.
 */
public class TestQueryTabStore extends ClusterTest {

  private static PersistentStoreProvider provider;
  private static WorkManager workManager;

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    provider = cluster.drillbit().getContext().getStoreProvider();
    workManager = cluster.drillbit().getManager();
  }

  private static QueryTabStore.TabRecord newRecord(
      String id, String owner, String projectId, String sql) {
    QueryTabStore.TabRecord r = new QueryTabStore.TabRecord();
    r.setId(id);
    r.setOwner(owner);
    r.setProjectId(projectId);
    r.setName("Query " + id);
    r.setSql(sql);
    r.setCreatedAt(System.currentTimeMillis());
    r.setUpdatedAt(System.currentTimeMillis());
    return r;
  }

  @Test
  public void testSaveAndListScopedByOwnerAndProject() {
    QueryTabStore store = QueryTabStore.get(provider, workManager);

    store.save(newRecord("t1", "alice", "proj1", "SELECT 1"));
    store.save(newRecord("t2", "bob", "proj1", "SELECT 2"));
    store.save(newRecord("t3", "alice", "proj2", "SELECT 3"));

    List<QueryTabStore.TabRecord> found = store.list("alice", "proj1");
    assertEquals(1, found.size());
    assertEquals("t1", found.get(0).getId());
  }

  @Test
  public void testGlobalTabsAreScopedToTheUser() {
    QueryTabStore store = QueryTabStore.get(provider, workManager);

    store.save(newRecord("g1", "alice", null, "SELECT 1"));
    store.save(newRecord("g2", "bob", null, "SELECT 2"));

    List<QueryTabStore.TabRecord> found = store.list("alice", null);
    assertEquals(1, found.size());
    assertEquals("g1", found.get(0).getId());
  }

  @Test
  public void testGlobalTabsAreSeparateFromProjectTabs() {
    QueryTabStore store = QueryTabStore.get(provider, workManager);

    store.save(newRecord("sep-global", "carol", null, "SELECT 1"));
    store.save(newRecord("sep-scoped", "carol", "projX", "SELECT 2"));

    assertEquals(1, store.list("carol", null).size());
    assertEquals("sep-global", store.list("carol", null).get(0).getId());
    assertEquals(1, store.list("carol", "projX").size());
    assertEquals("sep-scoped", store.list("carol", "projX").get(0).getId());
  }

  @Test
  public void testDeleteRemovesOnlyTheNamedTab() {
    QueryTabStore store = QueryTabStore.get(provider, workManager);

    store.save(newRecord("d1", "dave", "proj1", "SELECT 1"));
    store.save(newRecord("d2", "dave", "proj1", "SELECT 2"));

    store.delete("d1");

    List<QueryTabStore.TabRecord> found = store.list("dave", "proj1");
    assertEquals(1, found.size());
    assertEquals("d2", found.get(0).getId());
    assertNull(store.find("d1"));
  }

  @Test
  public void testFindLocatesATabByIdAlone() {
    QueryTabStore store = QueryTabStore.get(provider, workManager);
    store.save(newRecord("f1", "erin", "proj1", "SELECT 1"));

    QueryTabStore.TabRecord found = store.find("f1");
    assertEquals("erin", found.getOwner());
    assertEquals("proj1", found.getProjectId());
  }

  /**
   * The key joins owner, project and tab id with a NUL separator. NUL cannot appear
   * in a username, so no caller can craft an owner string that resolves into another
   * user's namespace the way a printable separator such as ':' would allow.
   */
  @Test
  public void testOwnerCannotForgeAKeyIntoAnotherNamespace() {
    QueryTabStore store = QueryTabStore.get(provider, workManager);
    store.save(newRecord("k1", "frank", "proj1", "SELECT 1"));

    assertTrue(store.list("frank" + '\0' + "proj1", "").isEmpty());
  }

  @Test
  public void testSavingTwiceUpdatesRatherThanDuplicates() {
    QueryTabStore store = QueryTabStore.get(provider, workManager);

    QueryTabStore.TabRecord record = newRecord("u1", "grace", "proj1", "SELECT 1");
    store.save(record);
    record.setSql("SELECT 2");
    store.save(record);

    List<QueryTabStore.TabRecord> found = store.list("grace", "proj1");
    assertEquals(1, found.size());
    assertEquals("SELECT 2", found.get(0).getSql());
  }
}
