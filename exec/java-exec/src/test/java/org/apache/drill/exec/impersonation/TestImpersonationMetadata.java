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
package org.apache.drill.exec.impersonation;

import org.apache.drill.categories.SecurityTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.exec.dotdrill.DotDrillType;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginException;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.drill.shaded.guava.com.google.common.base.Joiner;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.drill.test.ClientFixture;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Map;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests impersonation on metadata related queries as SHOW FILES, SHOW TABLES, CREATE VIEW, CREATE TABLE and DROP TABLE
 */
@Category({SlowTest.class, SecurityTest.class})
public class TestImpersonationMetadata extends BaseTestImpersonation {
  private static final String user1 = "drillTestUser1";
  private static final String user2 = "drillTestUser2";

  private static final String group0 = "drill_test_grp_0";
  private static final String group1 = "drill_test_grp_1";

  static {
    UserGroupInformation.createUserForTesting(user1, new String[]{ group1, group0 });
    UserGroupInformation.createUserForTesting(user2, new String[]{ group1 });
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setup() throws Exception {
    startMiniDfsCluster(TestImpersonationMetadata.class.getSimpleName());
    startDrillCluster(true);
    addMiniDfsBasedStorage(createTestWorkspaces());
  }

  private static Map<String, WorkspaceConfig> createTestWorkspaces() throws Exception {
    // Create "/tmp" folder and set permissions to "777"
    final Path tmpPath = new Path("/tmp");
    fs.delete(tmpPath, true);
    FileSystem.mkdirs(fs, tmpPath, new FsPermission((short)0777));

    Map<String, WorkspaceConfig> workspaces = Maps.newHashMap();

    // Create /drill_test_grp_0_700 directory with permissions 700 (owned by user running the tests)
    createAndAddWorkspace("drill_test_grp_0_700", "/drill_test_grp_0_700", (short)0700, processUser, group0, workspaces);

    // Create /drill_test_grp_0_750 directory with permissions 750 (owned by user running the tests)
    createAndAddWorkspace("drill_test_grp_0_750", "/drill_test_grp_0_750", (short)0750, processUser, group0, workspaces);

    // Create /drill_test_grp_0_755 directory with permissions 755 (owned by user running the tests)
    createAndAddWorkspace("drill_test_grp_0_755", "/drill_test_grp_0_755", (short)0755, processUser, group0, workspaces);

    // Create /drill_test_grp_0_770 directory with permissions 770 (owned by user running the tests)
    createAndAddWorkspace("drill_test_grp_0_770", "/drill_test_grp_0_770", (short)0770, processUser, group0, workspaces);

    // Create /drill_test_grp_0_777 directory with permissions 777 (owned by user running the tests)
    createAndAddWorkspace("drill_test_grp_0_777", "/drill_test_grp_0_777", (short)0777, processUser, group0, workspaces);

    // Create /drill_test_grp_1_700 directory with permissions 700 (owned by user1)
    createAndAddWorkspace("drill_test_grp_1_700", "/drill_test_grp_1_700", (short)0700, user1, group1, workspaces);

    // create /user2_workspace1 with 775 permissions (owner by user1)
    createAndAddWorkspace("user2_workspace1", "/user2_workspace1", (short)0775, user2, group1, workspaces);

    // create /user2_workspace with 755 permissions (owner by user1)
    createAndAddWorkspace("user2_workspace2", "/user2_workspace2", (short)0755, user2, group1, workspaces);

    return workspaces;
  }

  @Test
  public void testDropTable() throws Exception {

    // create tables as user2
    try (ClientFixture client = cluster.client(user2, "")) {
      client.run("use `%s.user2_workspace1`", MINI_DFS_STORAGE_PLUGIN_NAME);
      // create a table that can be dropped by another user in a different group
      client.run("create table parquet_table_775 as select * from cp.`employee.json`");

      // create a table that cannot be dropped by another user
      client.run("use `%s.user2_workspace2`", MINI_DFS_STORAGE_PLUGIN_NAME);
      client.run("create table parquet_table_700 as select * from cp.`employee.json`");
    }

    // Drop tables as user1
    try (ClientFixture client = cluster.client(user1, "")) {
      client.run("use `%s.user2_workspace1`", MINI_DFS_STORAGE_PLUGIN_NAME);
      client.testBuilder()
          .sqlQuery("drop table parquet_table_775")
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Table [%s] dropped", "parquet_table_775"))
          .go();

      client.run("use `%s.user2_workspace2`", MINI_DFS_STORAGE_PLUGIN_NAME);
      boolean dropFailed = false;
      try {
        client.run("drop table parquet_table_700");
      } catch (UserException e) {
        assertTrue(e.getMessage().contains("PERMISSION ERROR"));
        dropFailed = true;
      }
      assertTrue("Permission checking failed during drop table", dropFailed);
    }
  }

  @Test // DRILL-3037
  @Category(UnlikelyTest.class)
  public void testImpersonatingProcessUser() throws Exception {
    try (ClientFixture client = cluster.client(processUser, "")) {

      // Process user start the mini dfs, he has read/write permissions by default
      final String viewName = String.format("%s.drill_test_grp_0_700.testView", MINI_DFS_STORAGE_PLUGIN_NAME);
      try {
        client.run("CREATE VIEW " + viewName + " AS SELECT * FROM cp.`region.json`");
        client.run("SELECT * FROM " + viewName + " LIMIT 2");
      } finally {
        client.run("DROP VIEW " + viewName);
      }
    }
  }

  @Test
  public void testShowFilesInWSWithUserAndGroupPermissionsForQueryUser() throws Exception {
    try (ClientFixture client = cluster.client(user1, "")) {
      {
        // Try show tables in schema "drill_test_grp_1_700" which is owned by
        // "user1"
        List<QueryDataBatch> results = client
          .queryBuilder()
          .sql("SHOW FILES IN %s.drill_test_grp_1_700", MINI_DFS_STORAGE_PLUGIN_NAME)
          .results();
        assertTrue(client.countResults(results) > 0);
        results.forEach(r -> r.release());
      }
      {
        // Try show tables in schema "drill_test_grp_0_750" which is owned by
        // "processUser" and has group permissions for "user1"
        List<QueryDataBatch> results = client
          .queryBuilder()
          .sql("SHOW FILES IN %s.drill_test_grp_0_750", MINI_DFS_STORAGE_PLUGIN_NAME)
          .results();
        assertTrue(client.countResults(results) > 0);
        results.forEach(r -> r.release());
      }
    }
  }

  @Test
  public void testShowFilesInWSWithOtherPermissionsForQueryUser() throws Exception {
    try (ClientFixture client = cluster.client(user2, "")) {
      // Try show tables in schema "drill_test_grp_0_755" which is owned by "processUser" and group0. "user2" is not part of the "group0"
      List<QueryDataBatch> results = client.queryBuilder().sql(
        String.format("SHOW FILES IN %s.drill_test_grp_0_755", MINI_DFS_STORAGE_PLUGIN_NAME)).results();
      assertTrue(client.countResults(results) > 0);
      results.forEach(r -> r.release());
    }
  }

  @Test
  public void testShowFilesInWSWithNoPermissionsForQueryUser() throws Exception {
    try (ClientFixture client = cluster.client(user2, "")) {
      // Try show tables in schema "drill_test_grp_1_700" which is owned by "user1"
      List<QueryDataBatch> results = client.queryBuilder().sql(
        String.format("SHOW FILES IN %s.drill_test_grp_1_700", MINI_DFS_STORAGE_PLUGIN_NAME)).results();
      assertEquals(0, client.countResults(results));
      results.forEach(r -> r.release());
    }
  }

  @Test
  public void testShowSchemasAsUser1() throws Exception {
    // "user1" is part of "group0" and has access to following workspaces
    // drill_test_grp_1_700 (through ownership)
    // drill_test_grp_0_750, drill_test_grp_0_770 (through "group" category permissions)
    // drill_test_grp_0_755, drill_test_grp_0_777 (through "others" category permissions)
    try (ClientFixture client = cluster.client(user1, "")) {
      client.testBuilder()
          .sqlQuery("SHOW SCHEMAS LIKE '%drill_test%'")
          .unOrdered()
          .baselineColumns("SCHEMA_NAME")
          .baselineValues(String.format("%s.drill_test_grp_0_750", MINI_DFS_STORAGE_PLUGIN_NAME))
          .baselineValues(String.format("%s.drill_test_grp_0_755", MINI_DFS_STORAGE_PLUGIN_NAME))
          .baselineValues(String.format("%s.drill_test_grp_0_770", MINI_DFS_STORAGE_PLUGIN_NAME))
          .baselineValues(String.format("%s.drill_test_grp_0_777", MINI_DFS_STORAGE_PLUGIN_NAME))
          .baselineValues(String.format("%s.drill_test_grp_1_700", MINI_DFS_STORAGE_PLUGIN_NAME))
          .go();
    }
  }

  @Test
  public void testShowSchemasAsUser2() throws Exception {
    // "user2" is part of "group0", but part of "group1" and has access to following workspaces
    // drill_test_grp_0_755, drill_test_grp_0_777 (through "others" category permissions)
    try (ClientFixture client = cluster.client(user2, "")) {
      client.testBuilder()
          .sqlQuery("SHOW SCHEMAS LIKE '%drill_test%'")
          .unOrdered()
          .baselineColumns("SCHEMA_NAME")
          .baselineValues(String.format("%s.drill_test_grp_0_755", MINI_DFS_STORAGE_PLUGIN_NAME))
          .baselineValues(String.format("%s.drill_test_grp_0_777", MINI_DFS_STORAGE_PLUGIN_NAME))
          .go();
    }
  }

  @Test
  public void testCreateViewInDirWithUserPermissionsForQueryUser() throws Exception {
    final String viewSchema = MINI_DFS_STORAGE_PLUGIN_NAME + ".drill_test_grp_1_700"; // Workspace dir owned by "user1"
    testCreateViewTestHelper(user1, viewSchema, "view1");
  }

  @Test
  public void testCreateViewInDirWithGroupPermissionsForQueryUser() throws Exception {
    // Workspace dir owned by "processUser", workspace group is "group0" and "user1" is part of "group0"
    final String viewSchema = MINI_DFS_STORAGE_PLUGIN_NAME + ".drill_test_grp_0_770";
    testCreateViewTestHelper(user1, viewSchema, "view1");
  }

  @Test
  public void testCreateViewInDirWithOtherPermissionsForQueryUser() throws Exception {
    // Workspace dir owned by "processUser", workspace group is "group0" and "user2" is not part of "group0"
    final String viewSchema = MINI_DFS_STORAGE_PLUGIN_NAME + ".drill_test_grp_0_777";
    testCreateViewTestHelper(user2, viewSchema, "view1");
  }

  private static void testCreateViewTestHelper(String user, String viewSchema,
      String viewName) throws Exception {
    try (ClientFixture client = cluster.client(user, "")) {
      try {
        client.run("USE " + viewSchema);
        client.run("CREATE VIEW " + viewName + " AS SELECT " +
            "c_custkey, c_nationkey FROM cp.`tpch/customer.parquet` ORDER BY c_custkey");

        client.testBuilder()
            .sqlQuery("SHOW TABLES")
            .unOrdered()
            .baselineColumns("TABLE_SCHEMA", "TABLE_NAME")
            .baselineValues(viewSchema, viewName)
            .go();

        client.run("SHOW FILES");

        client.testBuilder()
            .sqlQuery("SELECT * FROM " + viewName + " LIMIT 1")
            .ordered()
            .baselineColumns("c_custkey", "c_nationkey")
            .baselineValues(1, 15)
            .go();

      } finally {
        client.run("DROP VIEW " + viewSchema + "." + viewName);
      }
    }
  }

  @Test
  public void testCreateViewInWSWithNoPermissionsForQueryUser() throws Exception {
    // Workspace dir owned by "processUser", workspace group is "group0" and "user2" is not part of "group0"
    final String tableWS = "drill_test_grp_0_755";
    final String viewSchema = MINI_DFS_STORAGE_PLUGIN_NAME + "." + tableWS;
    final String viewName = "view1";

    try (ClientFixture client = cluster.client(user2, "")) {

      client.run("USE " + viewSchema);

      String expErrorMsg = "PERMISSION ERROR: Permission denied: user=drillTestUser2, access=WRITE, inode=\"/" + tableWS;
      thrown.expect(UserRemoteException.class);
      thrown.expectMessage(containsString(expErrorMsg));

      client.run("CREATE VIEW %s AS" +
          " SELECT c_custkey, c_nationkey FROM cp.`tpch/customer.parquet` ORDER BY c_custkey", viewName);

      // SHOW TABLES is expected to return no records as view creation fails above.
      client.testBuilder()
          .sqlQuery("SHOW TABLES")
          .expectsEmptyResultSet()
          .go();

      client.run("SHOW FILES");
    }
  }

  @Test
  public void testCreateTableInDirWithUserPermissionsForQueryUser() throws Exception {
    final String tableWS = "drill_test_grp_1_700"; // Workspace dir owned by "user1"
    testCreateTableTestHelper(user1, tableWS, "table1");
  }

  @Test
  public void testCreateTableInDirWithGroupPermissionsForQueryUser() throws Exception {
    // Workspace dir owned by "processUser", workspace group is "group0" and "user1" is part of "group0"
    final String tableWS = "drill_test_grp_0_770";
    testCreateTableTestHelper(user1, tableWS, "table1");
  }

  @Test
  public void testCreateTableInDirWithOtherPermissionsForQueryUser() throws Exception {
    // Workspace dir owned by "processUser", workspace group is "group0" and "user2" is not part of "group0"
    final String tableWS = "drill_test_grp_0_777";
    testCreateTableTestHelper(user2, tableWS, "table1");
  }

  private static void testCreateTableTestHelper(String user, String tableWS,
      String tableName) throws Exception {
    try {
      try (ClientFixture client = cluster.client(user, "")) {

        client.run("USE " + Joiner.on(".").join(MINI_DFS_STORAGE_PLUGIN_NAME, tableWS));

        client.run("CREATE TABLE " + tableName + " AS SELECT " +
            "c_custkey, c_nationkey FROM cp.`tpch/customer.parquet` ORDER BY c_custkey");

        client.run("SHOW FILES");

        client.testBuilder()
            .sqlQuery("SELECT * FROM " + tableName + " LIMIT 1")
            .ordered()
            .baselineColumns("c_custkey", "c_nationkey")
            .baselineValues(1, 15)
            .go();
      }

    } finally {
      // There is no drop table, we need to delete the table directory through FileSystem object
      final Path tablePath = new Path(Path.SEPARATOR + tableWS + Path.SEPARATOR + tableName);
      if (fs.exists(tablePath)) {
        fs.delete(tablePath, true);
      }
    }
  }

  @Test
  public void testCreateTableInWSWithNoPermissionsForQueryUser() throws Exception {
    // Workspace dir owned by "processUser", workspace group is "group0" and "user2" is not part of "group0"
    String tableWS = "drill_test_grp_0_755";
    String tableName = "table1";

    try (ClientFixture client = cluster.client(user2, "")) {
      client.run("use %s.`%s`", MINI_DFS_STORAGE_PLUGIN_NAME, tableWS);

      thrown.expect(UserRemoteException.class);
      thrown.expectMessage(containsString("Permission denied: user=drillTestUser2, " +
          "access=WRITE, inode=\"/" + tableWS));

      client.run("CREATE TABLE %s AS SELECT c_custkey, c_nationkey " +
          "FROM cp.`tpch/customer.parquet` ORDER BY c_custkey", tableName);
    }
  }

  @Test
  public void testRefreshMetadata() throws Exception {
    final String tableName = "nation1";
    final String tableWS = "drill_test_grp_1_700";

    try (ClientFixture client = cluster.client(user1, "")) {
      client.run("USE " + Joiner.on(".").join(MINI_DFS_STORAGE_PLUGIN_NAME, tableWS));

      client.run("CREATE TABLE " + tableName + " partition by (n_regionkey) AS SELECT * " +
                "FROM cp.`tpch/nation.parquet`");

      client.run( "refresh table metadata " + tableName);

      client.run("SELECT * FROM " + tableName);

      final Path tablePath = new Path(Path.SEPARATOR + tableWS + Path.SEPARATOR + tableName);
      assertTrue ( fs.exists(tablePath) && fs.isDirectory(tablePath));
      fs.mkdirs(new Path(tablePath, "tmp5"));

      client.run("SELECT * from " + tableName);
    }
  }

  @Test
  public void testAnalyzeTable() throws Exception {
    final String tableName = "nation1_stats";
    final String tableWS = "drill_test_grp_1_700";

    try (ClientFixture client = cluster.client(user1, "")) {
      client.run("USE " + Joiner.on(".").join(MINI_DFS_STORAGE_PLUGIN_NAME, tableWS));
      client.run("ALTER SESSION SET `store.format` = 'parquet'");
      client.run("CREATE TABLE " + tableName + " AS SELECT * FROM cp.`tpch/nation.parquet`");
      client.run("ANALYZE TABLE " + tableName + " COMPUTE STATISTICS");
      client.run("SELECT * FROM " + tableName);

      final Path statsFilePath = new Path(Path.SEPARATOR + tableWS + Path.SEPARATOR + tableName
          + Path.SEPARATOR + DotDrillType.STATS.getEnding());
      assertTrue (fs.exists(statsFilePath) && fs.isDirectory(statsFilePath));
      FileStatus status = fs.getFileStatus(statsFilePath);
      // Verify process user is the directory owner
      assert(processUser.equalsIgnoreCase(status.getOwner()));

      fs.mkdirs(new Path(statsFilePath, "tmp5"));

      client.run("SELECT * from " + tableName);
      client.run("DROP TABLE " + tableName);
    }
  }

  @After
  public void removeMiniDfsBasedStorage() throws PluginException {
    cluster.storageRegistry().remove(MINI_DFS_STORAGE_PLUGIN_NAME);
    stopMiniDfsCluster();
  }
}
