/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.sql;

import com.google.common.collect.Lists;
import mockit.Mock;
import mockit.MockUp;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.drill.test.BaseTestQuery;
import org.apache.drill.categories.SqlTest;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.store.StorageStrategy;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.apache.drill.exec.util.StoragePluginTestUtils.DFS_PLUGIN_NAME;
import static org.apache.drill.exec.util.StoragePluginTestUtils.DFS_TMP_SCHEMA;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@Category(SqlTest.class)
public class TestCTTAS extends BaseTestQuery {

  private static final UUID session_id = UUID.nameUUIDFromBytes("sessionId".getBytes());
  private static final String temp2_wk = "tmp2";
  private static final String temp2_schema = String.format("%s.%s", DFS_PLUGIN_NAME, temp2_wk);

  private static FileSystem fs;
  private static FsPermission expectedFolderPermission;
  private static FsPermission expectedFilePermission;

  @BeforeClass
  public static void init() throws Exception {
    MockUp<UUID> uuidMockUp = mockRandomUUID(session_id);
    Properties testConfigurations = cloneDefaultTestConfigProperties();
    testConfigurations.put(ExecConstants.DEFAULT_TEMPORARY_WORKSPACE, DFS_TMP_SCHEMA);
    updateTestCluster(1, DrillConfig.create(testConfigurations));
    uuidMockUp.tearDown();

    File tmp2 = dirTestWatcher.makeSubDir(Paths.get("tmp2"));

    StoragePluginRegistry pluginRegistry = getDrillbitContext().getStorage();
    FileSystemConfig pluginConfig = (FileSystemConfig) pluginRegistry.getPlugin(DFS_PLUGIN_NAME).getConfig();
    pluginConfig.workspaces.put(temp2_wk, new WorkspaceConfig(tmp2.getAbsolutePath(), true, null, false));
    pluginRegistry.createOrUpdate(DFS_PLUGIN_NAME, pluginConfig, true);

    fs = getLocalFileSystem();
    expectedFolderPermission = new FsPermission(StorageStrategy.TEMPORARY.getFolderPermission());
    expectedFilePermission = new FsPermission(StorageStrategy.TEMPORARY.getFilePermission());
  }

  private static MockUp<UUID> mockRandomUUID(final UUID uuid) {
    return new MockUp<UUID>() {
      @Mock
      public UUID randomUUID() {
        return uuid;
      }
    };
  }

  @Test
  public void testSyntax() throws Exception {
    test("create TEMPORARY table temporary_keyword as select 1 from (values(1))");
    test("create TEMPORARY table %s.temporary_keyword_with_wk as select 1 from (values(1))", DFS_TMP_SCHEMA);
  }

  @Test
  public void testCreateTableWithDifferentStorageFormats() throws Exception {
    List<String> storageFormats = Lists.newArrayList("parquet", "json", "csvh");

    try {
      for (String storageFormat : storageFormats) {
        String temporaryTableName = "temp_" + storageFormat;
        mockRandomUUID(UUID.nameUUIDFromBytes(temporaryTableName.getBytes()));
        test("alter session set `store.format`='%s'", storageFormat);
        test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", temporaryTableName);
        checkPermission(temporaryTableName);

        testBuilder()
            .sqlQuery("select * from %s", temporaryTableName)
            .unOrdered()
            .baselineColumns("c1")
            .baselineValues("A")
            .go();

        testBuilder()
            .sqlQuery("select * from %s", temporaryTableName)
            .unOrdered()
            .sqlBaselineQuery("select * from %s.%s", DFS_TMP_SCHEMA, temporaryTableName)
            .go();
      }
    } finally {
      test("alter session reset `store.format`");
    }
  }

  @Test
  public void testTemporaryTablesCaseInsensitivity() throws Exception {
    String temporaryTableName = "tEmP_InSeNSiTiVe";
    List<String> temporaryTableNames = Lists.newArrayList(
        temporaryTableName,
        temporaryTableName.toLowerCase(),
        temporaryTableName.toUpperCase());

    test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", temporaryTableName);
    for (String tableName : temporaryTableNames) {
      testBuilder()
          .sqlQuery("select * from %s", tableName)
          .unOrdered()
          .baselineColumns("c1")
          .baselineValues("A")
          .go();
    }
  }

  @Test
  public void testResolveTemporaryTableWithPartialSchema() throws Exception {
    String temporaryTableName = "temporary_table_with_partial_schema";
    test("use %s", DFS_PLUGIN_NAME);
    test("create temporary table tmp.%s as select 'A' as c1 from (values(1))", temporaryTableName);

    testBuilder()
        .sqlQuery("select * from tmp.%s", temporaryTableName)
        .unOrdered()
        .baselineColumns("c1")
        .baselineValues("A")
        .go();
  }

  @Test
  public void testPartitionByWithTemporaryTables() throws Exception {
    String temporaryTableName = "temporary_table_with_partitions";
    mockRandomUUID(UUID.nameUUIDFromBytes(temporaryTableName.getBytes()));
    test("create TEMPORARY table %s partition by (c1) as select * from (" +
        "select 'A' as c1 from (values(1)) union all select 'B' as c1 from (values(1))) t", temporaryTableName);
    checkPermission(temporaryTableName);
  }

  @Test(expected = UserRemoteException.class)
  public void testCreationOutsideOfDefaultTemporaryWorkspace() throws Exception {
    try {
      String temporaryTableName = "temporary_table_outside_of_default_workspace";
      test("create TEMPORARY table %s.%s as select 'A' as c1 from (values(1))", temp2_schema, temporaryTableName);
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString(String.format(
          "VALIDATION ERROR: Temporary tables are not allowed to be created / dropped " +
              "outside of default temporary workspace [%s].", DFS_TMP_SCHEMA)));
      throw e;
    }
  }

  @Test(expected = UserRemoteException.class)
  public void testCreateWhenTemporaryTableExistsWithoutSchema() throws Exception {
    String temporaryTableName = "temporary_table_exists_without_schema";
    try {
      test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", temporaryTableName);
      test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", temporaryTableName);
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString(String.format(
         "VALIDATION ERROR: A table or view with given name [%s]" +
             " already exists in schema [%s]", temporaryTableName, DFS_TMP_SCHEMA)));
      throw e;
    }
  }

  @Test(expected = UserRemoteException.class)
  public void testCreateWhenTemporaryTableExistsCaseInsensitive() throws Exception {
    String temporaryTableName = "temporary_table_exists_without_schema";
    try {
      test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", temporaryTableName);
      test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", temporaryTableName.toUpperCase());
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString(String.format(
          "VALIDATION ERROR: A table or view with given name [%s]" +
              " already exists in schema [%s]", temporaryTableName.toUpperCase(), DFS_TMP_SCHEMA)));
      throw e;
    }
  }

  @Test(expected = UserRemoteException.class)
  public void testCreateWhenTemporaryTableExistsWithSchema() throws Exception {
    String temporaryTableName = "temporary_table_exists_with_schema";
    try {
      test("create TEMPORARY table %s.%s as select 'A' as c1 from (values(1))", DFS_TMP_SCHEMA, temporaryTableName);
      test("create TEMPORARY table %s.%s as select 'A' as c1 from (values(1))", DFS_TMP_SCHEMA, temporaryTableName);
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString(String.format(
          "VALIDATION ERROR: A table or view with given name [%s]" +
              " already exists in schema [%s]", temporaryTableName, DFS_TMP_SCHEMA)));
      throw e;
    }
  }

  @Test(expected = UserRemoteException.class)
  public void testCreateWhenPersistentTableExists() throws Exception {
    String persistentTableName = "persistent_table_exists";
    try {
      test("create table %s.%s as select 'A' as c1 from (values(1))", DFS_TMP_SCHEMA, persistentTableName);
      test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", persistentTableName);
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString(String.format(
          "VALIDATION ERROR: A table or view with given name [%s]" +
              " already exists in schema [%s]", persistentTableName, DFS_TMP_SCHEMA)));
      throw e;
    }
  }

  @Test(expected = UserRemoteException.class)
  public void testCreateWhenViewExists() throws Exception {
    String viewName = "view_exists";
    try {
      test("create view %s.%s as select 'A' as c1 from (values(1))", DFS_TMP_SCHEMA, viewName);
      test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", viewName);
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString(String.format(
          "VALIDATION ERROR: A table or view with given name [%s]" +
              " already exists in schema [%s]", viewName, DFS_TMP_SCHEMA)));
      throw e;
    }
  }

  @Test(expected = UserRemoteException.class)
  public void testCreatePersistentTableWhenTemporaryTableExists() throws Exception {
    String temporaryTableName = "temporary_table_exists_before_persistent";
    try {
      test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", temporaryTableName);
      test("create table %s.%s as select 'A' as c1 from (values(1))", DFS_TMP_SCHEMA, temporaryTableName);
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString(String.format(
          "VALIDATION ERROR: A table or view with given name [%s]" +
              " already exists in schema [%s]", temporaryTableName, DFS_TMP_SCHEMA)));
      throw e;
    }
  }

  @Test(expected = UserRemoteException.class)
  public void testCreateViewWhenTemporaryTableExists() throws Exception {
    String temporaryTableName = "temporary_table_exists_before_view";
    try {
      test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", temporaryTableName);
      test("create view %s.%s as select 'A' as c1 from (values(1))", DFS_TMP_SCHEMA, temporaryTableName);
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString(String.format(
          "VALIDATION ERROR: A non-view table with given name [%s] already exists in schema [%s]",
          temporaryTableName, DFS_TMP_SCHEMA)));
      throw e;
    }
  }

  @Test
  public void testSelectWithJoinOnTemporaryTables() throws Exception {
    String temporaryLeftTableName = "temporary_left_table_for_Select_with_join";
    String temporaryRightTableName = "temporary_right_table_for_Select_with_join";
    test("create TEMPORARY table %s as select 'A' as c1, 'B' as c2 from (values(1))", temporaryLeftTableName);
    test("create TEMPORARY table %s as select 'A' as c1, 'C' as c2 from (values(1))", temporaryRightTableName);

    testBuilder()
        .sqlQuery("select t1.c2 col1, t2.c2 col2 from %s t1 join %s t2 on t1.c1 = t2.c1", temporaryLeftTableName, temporaryRightTableName)
        .unOrdered()
        .baselineColumns("col1", "col2")
        .baselineValues("B", "C")
        .go();
  }

  @Test
  public void testTemporaryAndPersistentTablesPriority() throws Exception {
    String name = "temporary_and_persistent_table";
    test("use %s", temp2_schema);
    test("create TEMPORARY table %s as select 'temporary_table' as c1 from (values(1))", name);
    test("create table %s as select 'persistent_table' as c1 from (values(1))", name);

    testBuilder()
        .sqlQuery("select * from %s", name)
        .unOrdered()
        .baselineColumns("c1")
        .baselineValues("temporary_table")
        .go();

    testBuilder()
        .sqlQuery("select * from %s.%s", temp2_schema, name)
        .unOrdered()
        .baselineColumns("c1")
        .baselineValues("persistent_table")
        .go();

    test("drop table %s", name);

    testBuilder()
        .sqlQuery("select * from %s", name)
        .unOrdered()
        .baselineColumns("c1")
        .baselineValues("persistent_table")
        .go();
  }

  @Test
  public void testTemporaryTableAndViewPriority() throws Exception {
    String name = "temporary_table_and_view";
    test("use %s", temp2_schema);
    test("create TEMPORARY table %s as select 'temporary_table' as c1 from (values(1))", name);
    test("create view %s as select 'view' as c1 from (values(1))", name);

    testBuilder()
        .sqlQuery("select * from %s", name)
        .unOrdered()
        .baselineColumns("c1")
        .baselineValues("temporary_table")
        .go();

    testBuilder()
        .sqlQuery("select * from %s.%s", temp2_schema, name)
        .unOrdered()
        .baselineColumns("c1")
        .baselineValues("view")
        .go();

    test("drop table %s", name);

    testBuilder()
        .sqlQuery("select * from %s", name)
        .unOrdered()
        .baselineColumns("c1")
        .baselineValues("view")
        .go();
  }

  @Test(expected = UserRemoteException.class)
  public void testTemporaryTablesInViewDefinitions() throws Exception {
    String temporaryTableName = "temporary_table_for_view_definition";
    test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", temporaryTableName);

    try {
      test("create view %s.view_with_temp_table as select * from %s", DFS_TMP_SCHEMA, temporaryTableName);
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString(String.format(
          "VALIDATION ERROR: Temporary tables usage is disallowed. Used temporary table name: [%s]", temporaryTableName)));
      throw e;
    }
  }

  @Test(expected = UserRemoteException.class)
  public void testTemporaryTablesInViewExpansionLogic() throws Exception {
    String tableName = "table_for_expansion_logic_test";
    String viewName = "view_for_expansion_logic_test";
    test("use %s", DFS_TMP_SCHEMA);
    test("create table %s as select 'TABLE' as c1 from (values(1))", tableName);
    test("create view %s as select * from %s", viewName, tableName);

    testBuilder()
        .sqlQuery("select * from %s", viewName)
        .unOrdered()
        .baselineColumns("c1")
        .baselineValues("TABLE")
        .go();

    test("drop table %s", tableName);
    test("create temporary table %s as select 'TEMP' as c1 from (values(1))", tableName);
    try {
      test("select * from %s", viewName);
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString(String.format(
          "VALIDATION ERROR: Temporary tables usage is disallowed. Used temporary table name: [%s]", tableName)));
      throw e;
    }
  }

  @Test // DRILL-5952
  public void testCreateTemporaryTableIfNotExistsWhenTableWithSameNameAlreadyExists() throws Exception{
    final String newTblName = "createTemporaryTableIfNotExistsWhenATableWithSameNameAlreadyExists";

    try {
      String ctasQuery = String.format("CREATE TEMPORARY TABLE %s.%s AS SELECT * from cp.`region.json`", DFS_TMP_SCHEMA, newTblName);

      test(ctasQuery);

      ctasQuery =
        String.format("CREATE TEMPORARY TABLE IF NOT EXISTS %s AS SELECT * FROM cp.`employee.json`", newTblName);

      testBuilder()
        .sqlQuery(ctasQuery)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format("A table or view with given name [%s] already exists in schema [%s]", newTblName, DFS_TMP_SCHEMA))
        .go();
    } finally {
      test(String.format("DROP TABLE IF EXISTS %s.%s", DFS_TMP_SCHEMA, newTblName));
    }
  }

  @Test // DRILL-5952
  public void testCreateTemporaryTableIfNotExistsWhenViewWithSameNameAlreadyExists() throws Exception{
    final String newTblName = "createTemporaryTableIfNotExistsWhenAViewWithSameNameAlreadyExists";

    try {
      String ctasQuery = String.format("CREATE VIEW %s.%s AS SELECT * from cp.`region.json`", DFS_TMP_SCHEMA, newTblName);

      test(ctasQuery);

      ctasQuery =
        String.format("CREATE TEMPORARY TABLE IF NOT EXISTS %s.%s AS SELECT * FROM cp.`employee.json`", DFS_TMP_SCHEMA, newTblName);

      testBuilder()
        .sqlQuery(ctasQuery)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format("A table or view with given name [%s] already exists in schema [%s]", newTblName, DFS_TMP_SCHEMA))
        .go();
    } finally {
      test(String.format("DROP VIEW IF EXISTS %s.%s", DFS_TMP_SCHEMA, newTblName));
    }
  }

  @Test // DRILL-5952
  public void testCreateTemporaryTableIfNotExistsWhenTableWithSameNameDoesNotExist() throws Exception{
    final String newTblName = "createTemporaryTableIfNotExistsWhenATableWithSameNameDoesNotExist";

    try {
      String ctasQuery = String.format("CREATE TEMPORARY TABLE IF NOT EXISTS %s.%s AS SELECT * FROM cp.`employee.json`", DFS_TMP_SCHEMA, newTblName);

      test(ctasQuery);

    } finally {
      test(String.format("DROP TABLE IF EXISTS %s.%s", DFS_TMP_SCHEMA, newTblName));
    }
  }

  @Test
  public void testManualDropWithoutSchema() throws Exception {
    String temporaryTableName = "temporary_table_to_drop_without_schema";
    test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", temporaryTableName);

    testBuilder()
        .sqlQuery("drop table %s", temporaryTableName)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Temporary table [%s] dropped", temporaryTableName))
        .go();
  }

  @Test
  public void testManualDropWithSchema() throws Exception {
    String temporaryTableName = "temporary_table_to_drop_with_schema";
    test("create TEMPORARY table %s.%s as select 'A' as c1 from (values(1))", DFS_TMP_SCHEMA, temporaryTableName);

    testBuilder()
        .sqlQuery("drop table %s.%s", DFS_TMP_SCHEMA, temporaryTableName)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Temporary table [%s] dropped", temporaryTableName))
        .go();
  }

  @Test
  public void testDropTemporaryTableAsViewWithoutException() throws Exception {
    String temporaryTableName = "temporary_table_to_drop_like_view_without_exception";
    test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", temporaryTableName);

    testBuilder()
        .sqlQuery("drop view if exists %s.%s", DFS_TMP_SCHEMA, temporaryTableName)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format("View [%s] not found in schema [%s].",
            temporaryTableName, DFS_TMP_SCHEMA))
        .go();
  }

  @Test(expected = UserRemoteException.class)
  public void testDropTemporaryTableAsViewWithException() throws Exception {
    String temporaryTableName = "temporary_table_to_drop_like_view_with_exception";
    test("create TEMPORARY table %s as select 'A' as c1 from (values(1))", temporaryTableName);

    try {
      test("drop view %s.%s", DFS_TMP_SCHEMA, temporaryTableName);
    } catch (UserRemoteException e) {
      assertThat(e.getMessage(), containsString(String.format(
              "VALIDATION ERROR: Unknown view [%s] in schema [%s]", temporaryTableName, DFS_TMP_SCHEMA)));
      throw e;
    }
  }

  private void checkPermission(String tmpTableName) throws IOException {
    List<Path> matchingPath = findTemporaryTableLocation(tmpTableName);
    assertEquals("Only one directory should match temporary table name " + tmpTableName, 1, matchingPath.size());
    Path tmpTablePath = matchingPath.get(0);
    assertEquals("Directory permission should match",
        expectedFolderPermission, fs.getFileStatus(tmpTablePath).getPermission());
    RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(tmpTablePath, false);
    while (fileIterator.hasNext()) {
      assertEquals("File permission should match", expectedFilePermission, fileIterator.next().getPermission());
    }
  }

  private List<Path> findTemporaryTableLocation(String tableName) throws IOException {
    Path sessionTempLocation = new Path(dirTestWatcher.getDfsTestTmpDir().getAbsolutePath(), session_id.toString());
    assertTrue("Session temporary location must exist", fs.exists(sessionTempLocation));
    assertEquals("Session temporary location permission should match",
        expectedFolderPermission, fs.getFileStatus(sessionTempLocation).getPermission());
    String tableUUID =  UUID.nameUUIDFromBytes(tableName.getBytes()).toString();

    RemoteIterator<LocatedFileStatus> pathList = fs.listLocatedStatus(sessionTempLocation);
    List<Path> matchingPath = Lists.newArrayList();
    while (pathList.hasNext()) {
      LocatedFileStatus path = pathList.next();
      if (path.isDirectory() && path.getPath().getName().equals(tableUUID)) {
        matchingPath.add(path.getPath());
      }
    }
    return matchingPath;
  }
}
