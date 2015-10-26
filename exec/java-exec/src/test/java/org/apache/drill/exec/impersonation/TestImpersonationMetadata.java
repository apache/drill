/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.impersonation;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * Tests impersonation on metadata related queries as SHOW FILES, SHOW TABLES, CREATE VIEW, CREATE TABLE and DROP TABLE
 */
public class TestImpersonationMetadata extends BaseTestImpersonation {
  private static final String user1 = "drillTestUser1";
  private static final String user2 = "drillTestUser2";

  private static final String group0 = "drillTestGrp0";
  private static final String group1 = "drillTestGrp1";

  static {
    UserGroupInformation.createUserForTesting(user1, new String[]{ group1, group0 });
    UserGroupInformation.createUserForTesting(user2, new String[]{ group1 });
  }

  @BeforeClass
  public static void setup() throws Exception {
    startMiniDfsCluster(TestImpersonationMetadata.class.getSimpleName());
    startDrillCluster(true);
    addMiniDfsBasedStorage(createTestWorkspaces());
  }

  private static Map<String , WorkspaceConfig> createTestWorkspaces() throws Exception {
    // Create "/tmp" folder and set permissions to "777"
    final Path tmpPath = new Path("/tmp");
    fs.delete(tmpPath, true);
    FileSystem.mkdirs(fs, tmpPath, new FsPermission((short)0777));

    Map<String, WorkspaceConfig> workspaces = Maps.newHashMap();

    // Create /drillTestGrp0_700 directory with permissions 700 (owned by user running the tests)
    createAndAddWorkspace("drillTestGrp0_700", "/drillTestGrp0_700", (short)0700, processUser, group0, workspaces);

    // Create /drillTestGrp0_750 directory with permissions 750 (owned by user running the tests)
    createAndAddWorkspace("drillTestGrp0_750", "/drillTestGrp0_750", (short)0750, processUser, group0, workspaces);

    // Create /drillTestGrp0_755 directory with permissions 755 (owned by user running the tests)
    createAndAddWorkspace("drillTestGrp0_755", "/drillTestGrp0_755", (short)0755, processUser, group0, workspaces);

    // Create /drillTestGrp0_770 directory with permissions 770 (owned by user running the tests)
    createAndAddWorkspace("drillTestGrp0_770", "/drillTestGrp0_770", (short)0770, processUser, group0, workspaces);

    // Create /drillTestGrp0_777 directory with permissions 777 (owned by user running the tests)
    createAndAddWorkspace("drillTestGrp0_777", "/drillTestGrp0_777", (short)0777, processUser, group0, workspaces);

    // Create /drillTestGrp1_700 directory with permissions 700 (owned by user1)
    createAndAddWorkspace("drillTestGrp1_700", "/drillTestGrp1_700", (short)0700, user1, group1, workspaces);

    // create /user2_workspace1 with 775 permissions (owner by user1)
    createAndAddWorkspace("user2_workspace1", "/user2_workspace1", (short)0775, user2, group1, workspaces);

    // create /user2_workspace with 755 permissions (owner by user1)
    createAndAddWorkspace("user2_workspace2", "/user2_workspace2", (short)0755, user2, group1, workspaces);

    return workspaces;
  }

  @Test
  public void testDropTable() throws Exception {

    // create tables as user2
    updateClient(user2);
    test(String.format("use `%s.user2_workspace1`", MINIDFS_STORAGE_PLUGIN_NAME));
    // create a table that can be dropped by another user in a different group
    test("create table parquet_table_775 as select * from cp.`employee.json`");

    // create a table that cannot be dropped by another user
    test(String.format("use `%s.user2_workspace2`", MINIDFS_STORAGE_PLUGIN_NAME));
    test("create table parquet_table_700 as select * from cp.`employee.json`");

    // Drop tables as user1
    updateClient(user1);
    test(String.format("use `%s.user2_workspace1`", MINIDFS_STORAGE_PLUGIN_NAME));
    testBuilder()
        .sqlQuery("drop table parquet_table_775")
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Table [%s] dropped", "parquet_table_775"))
        .go();

    test(String.format("use `%s.user2_workspace2`", MINIDFS_STORAGE_PLUGIN_NAME));
    boolean dropFailed = false;
    try {
      test("drop table parquet_table_700");
    } catch (UserException e) {
      Assert.assertTrue(e.getMessage().contains("PERMISSION ERROR"));
      dropFailed = true;
    }
    Assert.assertTrue("Permission checking failed during drop table", dropFailed);
  }

  @Test // DRILL-3037
  public void testImpersonatingProcessUser() throws Exception {
    updateClient(processUser);

    // Process user start the mini dfs, he has read/write permissions by default
    final String viewName = String.format("%s.drillTestGrp0_700.testView", MINIDFS_STORAGE_PLUGIN_NAME);
    try {
      test("CREATE VIEW " + viewName + " AS SELECT * FROM cp.`region.json`");
      test("SELECT * FROM " + viewName + " LIMIT 2");
    } finally {
      test("DROP VIEW " + viewName);
    }
  }

  @Test
  public void testShowFilesInWSWithUserAndGroupPermissionsForQueryUser() throws Exception {
    updateClient(user1);

    // Try show tables in schema "drillTestGrp1_700" which is owned by "user1"
    test(String.format("SHOW FILES IN %s.drillTestGrp1_700", MINIDFS_STORAGE_PLUGIN_NAME));

    // Try show tables in schema "drillTestGrp0_750" which is owned by "processUser" and has group permissions for
    // "user1"
    test(String.format("SHOW FILES IN %s.drillTestGrp0_750", MINIDFS_STORAGE_PLUGIN_NAME));
  }

  @Test
  public void testShowFilesInWSWithOtherPermissionsForQueryUser() throws Exception {
    updateClient(user2);
    // Try show tables in schema "drillTestGrp0_755" which is owned by "processUser" and group0. "user2" is not part
    // of the "group0"
    test(String.format("SHOW FILES IN %s.drillTestGrp0_755", MINIDFS_STORAGE_PLUGIN_NAME));
  }

  @Test
  public void testShowFilesInWSWithNoPermissionsForQueryUser() throws Exception {
    UserRemoteException ex = null;

    updateClient(user2);
    try {
      // Try show tables in schema "drillTestGrp1_700" which is owned by "user1"
      test(String.format("SHOW FILES IN %s.drillTestGrp1_700", MINIDFS_STORAGE_PLUGIN_NAME));
    } catch(UserRemoteException e) {
      ex = e;
    }

    assertNotNull("UserRemoteException is expected", ex);
    assertThat(ex.getMessage(),
        containsString("Permission denied: user=drillTestUser2, " +
            "access=READ_EXECUTE, inode=\"/drillTestGrp1_700\":drillTestUser1:drillTestGrp1:drwx------"));
  }

  @Test
  public void testShowSchemasAsUser1() throws Exception {
    // "user1" is part of "group0" and has access to following workspaces
    // drillTestGrp1_700 (through ownership)
    // drillTestGrp0_750, drillTestGrp0_770 (through "group" category permissions)
    // drillTestGrp0_755, drillTestGrp0_777 (through "others" category permissions)
    updateClient(user1);
    testBuilder()
        .sqlQuery("SHOW SCHEMAS LIKE '%drillTest%'")
        .unOrdered()
        .baselineColumns("SCHEMA_NAME")
        .baselineValues(String.format("%s.drillTestGrp0_750", MINIDFS_STORAGE_PLUGIN_NAME))
        .baselineValues(String.format("%s.drillTestGrp0_755", MINIDFS_STORAGE_PLUGIN_NAME))
        .baselineValues(String.format("%s.drillTestGrp0_770", MINIDFS_STORAGE_PLUGIN_NAME))
        .baselineValues(String.format("%s.drillTestGrp0_777", MINIDFS_STORAGE_PLUGIN_NAME))
        .baselineValues(String.format("%s.drillTestGrp1_700", MINIDFS_STORAGE_PLUGIN_NAME))
        .go();
  }

  @Test
  public void testShowSchemasAsUser2() throws Exception {
    // "user2" is part of "group0", but part of "group1" and has access to following workspaces
    // drillTestGrp0_755, drillTestGrp0_777 (through "others" category permissions)
    updateClient(user2);
    testBuilder()
        .sqlQuery("SHOW SCHEMAS LIKE '%drillTest%'")
        .unOrdered()
        .baselineColumns("SCHEMA_NAME")
        .baselineValues(String.format("%s.drillTestGrp0_755", MINIDFS_STORAGE_PLUGIN_NAME))
        .baselineValues(String.format("%s.drillTestGrp0_777", MINIDFS_STORAGE_PLUGIN_NAME))
        .go();
  }

  @Test
  public void testCreateViewInDirWithUserPermissionsForQueryUser() throws Exception {
    final String viewSchema = MINIDFS_STORAGE_PLUGIN_NAME + ".drillTestGrp1_700"; // Workspace dir owned by "user1"
    testCreateViewTestHelper(user1, viewSchema, "view1");
  }

  @Test
  public void testCreateViewInDirWithGroupPermissionsForQueryUser() throws Exception {
    // Workspace dir owned by "processUser", workspace group is "group0" and "user1" is part of "group0"
    final String viewSchema = MINIDFS_STORAGE_PLUGIN_NAME + ".drillTestGrp0_770";
    testCreateViewTestHelper(user1, viewSchema, "view1");
  }

  @Test
  public void testCreateViewInDirWithOtherPermissionsForQueryUser() throws Exception {
    // Workspace dir owned by "processUser", workspace group is "group0" and "user2" is not part of "group0"
    final String viewSchema = MINIDFS_STORAGE_PLUGIN_NAME + ".drillTestGrp0_777";
    testCreateViewTestHelper(user2, viewSchema, "view1");
  }

  private static void testCreateViewTestHelper(String user, String viewSchema,
      String viewName) throws Exception {
    try {
      updateClient(user);

      test("USE " + viewSchema);

      test("CREATE VIEW " + viewName + " AS SELECT " +
          "c_custkey, c_nationkey FROM cp.`tpch/customer.parquet` ORDER BY c_custkey;");

      testBuilder()
          .sqlQuery("SHOW TABLES")
          .unOrdered()
          .baselineColumns("TABLE_SCHEMA", "TABLE_NAME")
          .baselineValues(viewSchema, viewName)
          .go();

      test("SHOW FILES");

      testBuilder()
          .sqlQuery("SELECT * FROM " + viewName + " LIMIT 1")
          .ordered()
          .baselineColumns("c_custkey", "c_nationkey")
          .baselineValues(1, 15)
          .go();

    } finally {
      test("DROP VIEW " + viewSchema + "." + viewName);
    }
  }

  @Test
  public void testCreateViewInWSWithNoPermissionsForQueryUser() throws Exception {
    // Workspace dir owned by "processUser", workspace group is "group0" and "user2" is not part of "group0"
    final String viewSchema = MINIDFS_STORAGE_PLUGIN_NAME + ".drillTestGrp0_755";
    final String viewName = "view1";

    updateClient(user2);

    test("USE " + viewSchema);

    final String query = "CREATE VIEW " + viewName + " AS SELECT " +
        "c_custkey, c_nationkey FROM cp.`tpch/customer.parquet` ORDER BY c_custkey;";
    final String expErrorMsg = "PERMISSION ERROR: Permission denied: user=drillTestUser2, access=WRITE, inode=\"/drillTestGrp0_755/";
    errorMsgTestHelper(query, expErrorMsg);

    // SHOW TABLES is expected to return no records as view creation fails above.
    testBuilder()
        .sqlQuery("SHOW TABLES")
        .expectsEmptyResultSet()
        .go();

    test("SHOW FILES");
  }

  @Test
  public void testCreateTableInDirWithUserPermissionsForQueryUser() throws Exception {
    final String tableWS = "drillTestGrp1_700"; // Workspace dir owned by "user1"
    testCreateTableTestHelper(user1, tableWS, "table1");
  }

  @Test
  public void testCreateTableInDirWithGroupPermissionsForQueryUser() throws Exception {
    // Workspace dir owned by "processUser", workspace group is "group0" and "user1" is part of "group0"
    final String tableWS = "drillTestGrp0_770";
    testCreateTableTestHelper(user1, tableWS, "table1");
  }

  @Test
  public void testCreateTableInDirWithOtherPermissionsForQueryUser() throws Exception {
    // Workspace dir owned by "processUser", workspace group is "group0" and "user2" is not part of "group0"
    final String tableWS = "drillTestGrp0_777";
    testCreateTableTestHelper(user2, tableWS, "table1");
  }

  private static void testCreateTableTestHelper(String user, String tableWS,
      String tableName) throws Exception {
    try {
      updateClient(user);

      test("USE " + Joiner.on(".").join(MINIDFS_STORAGE_PLUGIN_NAME, tableWS));

      test("CREATE TABLE " + tableName + " AS SELECT " +
          "c_custkey, c_nationkey FROM cp.`tpch/customer.parquet` ORDER BY c_custkey;");

      test("SHOW FILES");

      testBuilder()
          .sqlQuery("SELECT * FROM " + tableName + " LIMIT 1")
          .ordered()
          .baselineColumns("c_custkey", "c_nationkey")
          .baselineValues(1, 15)
          .go();

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
    final String tableWS = "drillTestGrp0_755";
    final String tableName = "table1";

    UserRemoteException ex = null;

    try {
      updateClient(user2);

      test("USE " + Joiner.on(".").join(MINIDFS_STORAGE_PLUGIN_NAME, tableWS));

      test("CREATE TABLE " + tableName + " AS SELECT " +
          "c_custkey, c_nationkey FROM cp.`tpch/customer.parquet` ORDER BY c_custkey;");
    } catch(UserRemoteException e) {
      ex = e;
    }

    assertNotNull("UserRemoteException is expected", ex);
    assertThat(ex.getMessage(),
        containsString("SYSTEM ERROR: RemoteException: Permission denied: user=drillTestUser2, access=WRITE, inode=\"/drillTestGrp0_755/"));
  }

  @AfterClass
  public static void removeMiniDfsBasedStorage() throws Exception {
    getDrillbitContext().getStorage().deletePlugin(MINIDFS_STORAGE_PLUGIN_NAME);
    stopMiniDfsCluster();
  }
}
