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
package org.apache.drill.exec.impersonation.hive;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;

import org.apache.calcite.schema.Schema.TableType;
import org.apache.drill.categories.HiveStorageTest;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.drill.categories.SlowTest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.security.HadoopDefaultMetastoreAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener;
import org.apache.hadoop.hive.ql.security.authorization.StorageBasedAuthorizationProvider;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;
import java.util.Map;

import static org.apache.drill.exec.hive.HiveTestUtilities.executeQuery;
import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_CBO_ENABLED;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_METASTORE_AUTHENTICATOR_MANAGER;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_METASTORE_AUTHORIZATION_AUTH_READS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_METASTORE_AUTHORIZATION_MANAGER;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_AUTO_CREATE_ALL;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_PRE_EVENT_LISTENERS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.DYNAMICPARTITIONINGMODE;

@Category({SlowTest.class, HiveStorageTest.class})
public class TestStorageBasedHiveAuthorization extends BaseTestHiveImpersonation {

  // DB whose warehouse directory has permissions 755, available everyone to read
  private static final String db_general = "db_general";

  // Tables in "db_general"
  private static final String g_student_u0_700 = "student_u0_700";
  private static final String g_student_u0g0_750 = "student_u0g0_750";
  private static final String g_student_all_755 = "student_all_755";
  private static final String g_voter_u1_700 = "voter_u1_700";
  private static final String g_voter_u2g1_750 = "voter_u2g1_750";
  private static final String g_voter_all_755 = "voter_all_755";

  private static final String g_partitioned_student_u0_700 = "partitioned_student_u0_700";

  // DB whose warehouse directory has permissions 700 and owned by user0
  private static final String db_u0_only = "db_u0_only";

  // Tables in "db_u0_only"
  private static final String u0_student_all_755 = "student_all_755";
  private static final String u0_voter_all_755 = "voter_all_755";

  // DB whose warehouse directory has permissions 750 and owned by user1 and group1
  private static final String db_u1g1_only = "db_u1g1_only";

  // Tables in "db_u1g1_only"
  private static final String u1g1_student_all_755 = "student_all_755";
  private static final String u1g1_student_u1_700 = "student_u1_700";
  private static final String u1g1_voter_all_755 = "voter_all_755";
  private static final String u1g1_voter_u1_700 = "voter_u1_700";

  // Create a view on "student_u0_700". View is owned by user0:group0 and has permissions 750
  private static final String v_student_u0g0_750 = "v_student_u0g0_750";

  // Create a view on "v_student_u0g0_750". View is owned by user1:group1 and has permissions 750
  private static final String v_student_u1g1_750 = "v_student_u1g1_750";

  private static final String query_v_student_u0g0_750 = String.format(
      "SELECT rownum FROM %s.%s.%s ORDER BY rownum LIMIT 1", MINI_DFS_STORAGE_PLUGIN_NAME, "tmp", v_student_u0g0_750);

  private static final String query_v_student_u1g1_750 = String.format(
      "SELECT rownum FROM %s.%s.%s ORDER BY rownum LIMIT 1", MINI_DFS_STORAGE_PLUGIN_NAME, "tmp", v_student_u1g1_750);

  // Create a view on "partitioned_student_u0_700". View is owned by user0:group0 and has permissions 750
  private static final String v_partitioned_student_u0g0_750 = "v_partitioned_student_u0g0_750";

  // Create a view on "v_partitioned_student_u0g0_750". View is owned by user1:group1 and has permissions 750
  private static final String v_partitioned_student_u1g1_750 = "v_partitioned_student_u1g1_750";

  private static final String query_v_partitioned_student_u0g0_750 = String.format(
      "SELECT rownum FROM %s.%s.%s ORDER BY rownum LIMIT 1", MINI_DFS_STORAGE_PLUGIN_NAME, "tmp",
      v_partitioned_student_u0g0_750);

  private static final String query_v_partitioned_student_u1g1_750 = String.format(
      "SELECT rownum FROM %s.%s.%s ORDER BY rownum LIMIT 1", MINI_DFS_STORAGE_PLUGIN_NAME, "tmp",
      v_partitioned_student_u1g1_750);

  @BeforeClass
  public static void setup() throws Exception {
    startMiniDfsCluster(TestStorageBasedHiveAuthorization.class.getName());
    prepHiveConfAndData();
    setStorabaseBasedAuthorizationInHiveConf();
    startHiveMetaStore();
    startDrillCluster(true);
    addHiveStoragePlugin(getHivePluginConfig());
    addMiniDfsBasedStorage(Maps.<String, WorkspaceConfig>newHashMap());
    generateTestData();
  }

  private static void setStorabaseBasedAuthorizationInHiveConf() {
    // Turn on metastore-side authorization
    hiveConf.set(METASTORE_PRE_EVENT_LISTENERS.varname, AuthorizationPreEventListener.class.getName());
    hiveConf.set(HIVE_METASTORE_AUTHENTICATOR_MANAGER.varname, HadoopDefaultMetastoreAuthenticator.class.getName());
    hiveConf.set(HIVE_METASTORE_AUTHORIZATION_MANAGER.varname, StorageBasedAuthorizationProvider.class.getName());
    hiveConf.set(HIVE_METASTORE_AUTHORIZATION_AUTH_READS.varname, "true");
    hiveConf.set(METASTORE_EXECUTE_SET_UGI.varname, "true");
    hiveConf.set(DYNAMICPARTITIONINGMODE.varname, "nonstrict");
  }

  private static Map<String, String> getHivePluginConfig() {
    final Map<String, String> hiveConfig = Maps.newHashMap();
    hiveConfig.put(METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname));
    hiveConfig.put(FS_DEFAULT_NAME_KEY, dfsConf.get(FS_DEFAULT_NAME_KEY));
    hiveConfig.put(HIVE_SERVER2_ENABLE_DOAS.varname, hiveConf.get(HIVE_SERVER2_ENABLE_DOAS.varname));
    hiveConfig.put(METASTORE_EXECUTE_SET_UGI.varname, hiveConf.get(METASTORE_EXECUTE_SET_UGI.varname));
    hiveConfig.put(METASTORE_SCHEMA_VERIFICATION.varname, hiveConf.get(METASTORE_SCHEMA_VERIFICATION.varname));
    hiveConfig.put(METASTORE_AUTO_CREATE_ALL.varname, hiveConf.get(METASTORE_AUTO_CREATE_ALL.varname));
    hiveConfig.put(HIVE_CBO_ENABLED.varname, hiveConf.get(HIVE_CBO_ENABLED.varname));
    return hiveConfig;
  }

  private static void generateTestData() throws Exception {

    // Generate Hive test tables
    final SessionState ss = new SessionState(hiveConf);
    SessionState.start(ss);
    final Driver driver = new Driver(hiveConf);

    executeQuery(driver, "CREATE DATABASE " + db_general);

    createTable(driver,
        db_general, g_student_u0_700, studentDef, studentData, org1Users[0], org1Groups[0], (short) 0700);
    createTable(driver,
        db_general, g_student_u0g0_750, studentDef, studentData, org1Users[0], org1Groups[0], (short) 0750);
    createTable(driver,
        db_general, g_student_all_755, studentDef, studentData, org1Users[2], org1Groups[2], (short) 0755);
    createTable(driver,
        db_general, g_voter_u1_700, voterDef, voterData, org1Users[1], org1Groups[1], (short) 0700);
    createTable(driver,
        db_general, g_voter_u2g1_750, voterDef, voterData, org1Users[2], org1Groups[1], (short) 0750);
    createTable(driver,
        db_general, g_voter_all_755, voterDef, voterData, org1Users[1], org1Groups[1], (short) 0755);

    createPartitionedTable(driver,
        db_general, g_partitioned_student_u0_700, partitionStudentDef,
        "INSERT OVERWRITE TABLE %s.%s PARTITION(age) SELECT rownum, name, age, gpa, studentnum FROM %s.%s",
        g_student_all_755, org1Users[0], org1Groups[0], (short) 0700);

    changeDBPermissions(db_general, (short) 0755, org1Users[0], org1Groups[0]);

    executeQuery(driver, "CREATE DATABASE " + db_u1g1_only);

    createTable(driver,
        db_u1g1_only, u1g1_student_all_755, studentDef, studentData, org1Users[1], org1Groups[1], (short) 0755);
    createTable(driver,
        db_u1g1_only, u1g1_student_u1_700, studentDef, studentData, org1Users[1], org1Groups[1], (short) 0700);
    createTable(driver,
        db_u1g1_only, u1g1_voter_all_755, voterDef, voterData, org1Users[1], org1Groups[1], (short) 0755);
    createTable(driver,
        db_u1g1_only, u1g1_voter_u1_700, voterDef, voterData, org1Users[1], org1Groups[1], (short) 0700);

    changeDBPermissions(db_u1g1_only, (short) 0750, org1Users[1], org1Groups[1]);

    executeQuery(driver, "CREATE DATABASE " + db_u0_only);

    createTable(driver, db_u0_only, u0_student_all_755, studentDef, studentData, org1Users[0], org1Groups[0], (short) 0755);
    createTable(driver, db_u0_only, u0_voter_all_755, voterDef, voterData, org1Users[0], org1Groups[0], (short) 0755);

    changeDBPermissions(db_u0_only, (short) 0700, org1Users[0], org1Groups[0]);

    createView(org1Users[0], org1Groups[0], v_student_u0g0_750,
        String.format("SELECT rownum, name, age, studentnum FROM %s.%s.%s",
            hivePluginName, db_general, g_student_u0_700));

    createView(org1Users[1], org1Groups[1], v_student_u1g1_750,
        String.format("SELECT rownum, name, age FROM %s.%s.%s", MINI_DFS_STORAGE_PLUGIN_NAME, "tmp", v_student_u0g0_750));

    createView(org1Users[0], org1Groups[0], v_partitioned_student_u0g0_750,
        String.format("SELECT rownum, name, age, studentnum FROM %s.%s.%s",
            hivePluginName, db_general, g_partitioned_student_u0_700));

    createView(org1Users[1], org1Groups[1], v_partitioned_student_u1g1_750,
        String.format("SELECT rownum, name, age FROM %s.%s.%s", MINI_DFS_STORAGE_PLUGIN_NAME, "tmp", v_partitioned_student_u0g0_750));
  }

  private static void createPartitionedTable(final Driver hiveDriver, final String db, final String tbl,
      final String tblDef, final String loadTblDef, final String loadTbl, final String user, final String group,
      final short permissions) throws Exception {
    executeQuery(hiveDriver, String.format(tblDef, db, tbl));
    executeQuery(hiveDriver, String.format(loadTblDef, db, tbl, db, loadTbl));

    final Path p = getWhPathForHiveObject(db, tbl);
    fs.setPermission(p, new FsPermission(permissions));
    fs.setOwner(p, user, group);
  }

  private static void createTable(final Driver hiveDriver, final String db, final String tbl, final String tblDef,
      final String tblData, final String user, final String group, final short permissions) throws Exception {
    executeQuery(hiveDriver, String.format(tblDef, db, tbl));
    executeQuery(hiveDriver, String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE %s.%s", tblData, db, tbl));
    final Path p = getWhPathForHiveObject(db, tbl);
    fs.setPermission(p, new FsPermission(permissions));
    fs.setOwner(p, user, group);
  }

  private static void changeDBPermissions(final String db, final short perm, final String u, final String g)
      throws Exception {
    Path p = getWhPathForHiveObject(db, null);
    fs.setPermission(p, new FsPermission(perm));
    fs.setOwner(p, u, g);
  }

  // Irrespective of each db permissions, all dbs show up in "SHOW SCHEMAS"
  @Test
  public void showSchemas() throws Exception {
    testBuilder()
        .sqlQuery("SHOW SCHEMAS LIKE 'hive.%'")
        .unOrdered()
        .baselineColumns("SCHEMA_NAME")
        .baselineValues("hive.db_general")
        .baselineValues("hive.db_u0_only")
        .baselineValues("hive.db_u1g1_only")
        .baselineValues("hive.default")
        .go();
  }

  /**
   * "SHOW TABLE" output for a db, should only contain the tables that the user
   * has access to read. If the user has no read access to the db, the list will be always empty even if the user has
   * read access to the tables inside the db.
   * @throws Exception
   */
  @Test
  public void showTablesUser0() throws Exception {
    updateClient(org1Users[0]);

    showTablesHelper(db_general,
        ImmutableList.of(
            g_student_u0_700,
            g_student_u0g0_750,
            g_student_all_755,
            g_voter_all_755,
            g_partitioned_student_u0_700
        ));

    showTablesHelper(db_u0_only,
        ImmutableList.of(
            u0_student_all_755,
            u0_voter_all_755
        ));

    showTablesHelper(db_u1g1_only, Collections.<String>emptyList());
  }

  @Test
  public void fromInfoSchemaUser0() throws Exception {
    updateClient(org1Users[0]);

    fromInfoSchemaHelper(
        hivePluginName,
        db_general,
        ImmutableList.of(
            g_student_u0_700,
            g_student_u0g0_750,
            g_student_all_755,
            g_voter_all_755,
            g_partitioned_student_u0_700
        ),
        ImmutableList.of(
            TableType.TABLE,
            TableType.TABLE,
            TableType.TABLE,
            TableType.TABLE,
            TableType.TABLE
        ));

    fromInfoSchemaHelper(
        hivePluginName,
        db_u0_only,
        ImmutableList.of(
            u0_student_all_755,
            u0_voter_all_755
        ),
        ImmutableList.of(
            TableType.TABLE,
            TableType.TABLE
        ));

    fromInfoSchemaHelper(
        hivePluginName,
        db_u1g1_only,
        Collections.<String>emptyList(),
        Collections.<TableType>emptyList());
  }

  @Test
  public void showTablesUser1() throws Exception {
    updateClient(org1Users[1]);

    showTablesHelper(db_general,
        ImmutableList.of(
            g_student_u0g0_750,
            g_student_all_755,
            g_voter_u1_700,
            g_voter_u2g1_750,
            g_voter_all_755
        ));

    showTablesHelper(db_u1g1_only,
        ImmutableList.of(
            u1g1_student_all_755,
            u1g1_student_u1_700,
            u1g1_voter_all_755,
            u1g1_voter_u1_700
        ));

    showTablesHelper(db_u0_only, Collections.<String>emptyList());
  }

  @Test
  public void fromInfoSchemaUser1() throws Exception {
    updateClient(org1Users[1]);

    fromInfoSchemaHelper(
        hivePluginName,
        db_general,
        ImmutableList.of(
            g_student_u0g0_750,
            g_student_all_755,
            g_voter_u1_700,
            g_voter_u2g1_750,
            g_voter_all_755
        ),
        ImmutableList.of(
            TableType.TABLE,
            TableType.TABLE,
            TableType.TABLE,
            TableType.TABLE,
            TableType.TABLE
        ));

    fromInfoSchemaHelper(
        hivePluginName,
        db_u1g1_only,
        ImmutableList.of(
            u1g1_student_all_755,
            u1g1_student_u1_700,
            u1g1_voter_all_755,
            u1g1_voter_u1_700
        ),
        ImmutableList.of(
            TableType.TABLE,
            TableType.TABLE,
            TableType.TABLE,
            TableType.TABLE
        ));

    fromInfoSchemaHelper(
        hivePluginName,
        db_u0_only,
        Collections.<String>emptyList(),
        Collections.<TableType>emptyList());
  }

  @Test
  public void showTablesUser2() throws Exception {
    updateClient(org1Users[2]);

    showTablesHelper(db_general,
        ImmutableList.of(
            g_student_all_755,
            g_voter_u2g1_750,
            g_voter_all_755
        ));

    showTablesHelper(db_u1g1_only,
        ImmutableList.of(
            u1g1_student_all_755,
            u1g1_voter_all_755
        ));

    showTablesHelper(db_u0_only, Collections.<String>emptyList());
  }

  @Test
  public void fromInfoSchemaUser2() throws Exception {
    updateClient(org1Users[2]);

    fromInfoSchemaHelper(
        hivePluginName,
        db_general,
        ImmutableList.of(
            g_student_all_755,
            g_voter_u2g1_750,
            g_voter_all_755
        ),
        ImmutableList.of(
            TableType.TABLE,
            TableType.TABLE,
            TableType.TABLE
        ));

    fromInfoSchemaHelper(
        hivePluginName,
        db_u1g1_only,
        ImmutableList.of(
            u1g1_student_all_755,
            u1g1_voter_all_755
        ),
        ImmutableList.of(
            TableType.TABLE,
            TableType.TABLE
        ));

    fromInfoSchemaHelper(
        hivePluginName,
        db_u0_only,
        Collections.<String>emptyList(),
        Collections.<TableType>emptyList());
  }

  // Try to read the tables "user0" has access to read in db_general.
  @Test
  public void selectUser0_db_general() throws Exception {
    updateClient(org1Users[0]);

    test(String.format("SELECT * FROM hive.%s.%s ORDER BY gpa DESC LIMIT 2", db_general, g_student_u0_700));
    test(String.format("SELECT * FROM hive.%s.%s ORDER BY gpa DESC LIMIT 2", db_general, g_student_all_755));
    test(String.format("SELECT * FROM hive.%s.%s ORDER BY name DESC LIMIT 2", db_general, g_voter_all_755));

    test(String.format("SELECT * FROM hive.%s.%s ORDER BY gpa DESC LIMIT 2", db_general, g_partitioned_student_u0_700));
  }

  // Try to read the table that "user0" has access to read in db_u0_only
  @Test
  public void selectUser0_db_u0_only() throws Exception {
    updateClient(org1Users[0]);

    test(String.format("SELECT * FROM hive.%s.%s ORDER BY gpa DESC LIMIT 2", db_u0_only, u0_student_all_755));
    test(String.format("SELECT * FROM hive.%s.%s ORDER BY name DESC LIMIT 2", db_u0_only, u0_voter_all_755));
  }

  // Try to read the tables "user0" has no access to read in db_u1g1_only
  @Test
  public void selectUser0_db_u1g1_only() throws Exception {
    updateClient(org1Users[0]);

    errorMsgTestHelper(
        String.format("SELECT * FROM hive.%s.%s ORDER BY gpa DESC LIMIT 2", db_u1g1_only, u1g1_student_all_755),
        String.format("Object '%s' not found within 'hive.%s'", u1g1_student_all_755, db_u1g1_only));
  }

  // Try to read the tables "user1" has access to read in db_general.
  @Test
  public void selectUser1_db_general() throws Exception {
    updateClient(org1Users[1]);

    test(String.format("SELECT * FROM hive.%s.%s ORDER BY gpa DESC LIMIT 2", db_general, g_student_u0g0_750));
    test(String.format("SELECT * FROM hive.%s.%s ORDER BY gpa DESC LIMIT 2", db_general, g_student_all_755));
    test(String.format("SELECT * FROM hive.%s.%s ORDER BY name DESC LIMIT 2", db_general, g_voter_u2g1_750));
  }

  // Try to read the tables "user1" has no access to read in db_u0_only
  @Test
  public void selectUser1_db_u0_only() throws Exception {
    updateClient(org1Users[1]);

    errorMsgTestHelper(
        String.format("SELECT * FROM hive.%s.%s ORDER BY gpa DESC LIMIT 2", db_u0_only, u0_student_all_755),
        String.format("Object '%s' not found within 'hive.%s'", u0_student_all_755, db_u0_only));
  }

  private static void queryViewHelper(final String queryUser, final String query) throws Exception {
    updateClient(queryUser);
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("rownum")
        .baselineValues(1)
        .go();
  }

  @Test
  public void selectUser0_v_student_u0g0_750() throws Exception {
    queryViewHelper(org1Users[0], query_v_student_u0g0_750);
  }

  @Test
  public void selectUser1_v_student_u0g0_750() throws Exception {
    queryViewHelper(org1Users[1], query_v_student_u0g0_750);
  }

  @Test
  public void selectUser2_v_student_u0g0_750() throws Exception {
    updateClient(org1Users[2]);
    errorMsgTestHelper(query_v_student_u0g0_750, String.format(
        "Not authorized to read view [v_student_u0g0_750] in schema [%s.tmp]", MINI_DFS_STORAGE_PLUGIN_NAME));
  }

  @Test
  public void selectUser0_v_student_u1g1_750() throws Exception {
    updateClient(org1Users[0]);
    errorMsgTestHelper(query_v_student_u1g1_750, String.format(
        "Not authorized to read view [v_student_u1g1_750] in schema [%s.tmp]", MINI_DFS_STORAGE_PLUGIN_NAME));
  }

  @Test
  public void selectUser1_v_student_u1g1_750() throws Exception {
    queryViewHelper(org1Users[1], query_v_student_u1g1_750);
  }

  @Test
  public void selectUser2_v_student_u1g1_750() throws Exception {
    queryViewHelper(org1Users[2], query_v_student_u1g1_750);
  }

  @Test
  public void selectUser0_v_partitioned_student_u0g0_750() throws Exception {
    queryViewHelper(org1Users[0], query_v_partitioned_student_u0g0_750);
  }

  @Test
  public void selectUser1_v_partitioned_student_u0g0_750() throws Exception {
    queryViewHelper(org1Users[1], query_v_partitioned_student_u0g0_750);
  }

  @Test
  public void selectUser2_v_partitioned_student_u0g0_750() throws Exception {
    updateClient(org1Users[2]);
    errorMsgTestHelper(query_v_partitioned_student_u0g0_750, String.format(
        "Not authorized to read view [v_partitioned_student_u0g0_750] in schema [%s.tmp]", MINI_DFS_STORAGE_PLUGIN_NAME));
  }

  @Test
  public void selectUser0_v_partitioned_student_u1g1_750() throws Exception {
    updateClient(org1Users[0]);
    errorMsgTestHelper(query_v_partitioned_student_u1g1_750, String.format(
        "Not authorized to read view [v_partitioned_student_u1g1_750] in schema [%s.tmp]", MINI_DFS_STORAGE_PLUGIN_NAME));
  }

  @Test
  public void selectUser1_v_partitioned_student_u1g1_750() throws Exception {
    queryViewHelper(org1Users[1], query_v_partitioned_student_u1g1_750);
  }

  @Test
  public void selectUser2_v_partitioned_student_u1g1_750() throws Exception {
    queryViewHelper(org1Users[2], query_v_partitioned_student_u1g1_750);
  }

  @AfterClass
  public static void shutdown() throws Exception {
    stopMiniDfsCluster();
    stopHiveMetaStore();
  }
}
