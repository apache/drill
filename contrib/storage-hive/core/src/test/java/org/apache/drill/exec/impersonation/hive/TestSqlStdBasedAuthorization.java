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
import org.apache.drill.categories.HiveStorageTest;
import org.apache.drill.categories.SlowTest;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
import org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdConfOnlyAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

import static org.apache.drill.exec.hive.HiveTestUtilities.executeQuery;
import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_CBO_ENABLED;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_AUTO_CREATE_ALL;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION;

@Category({SlowTest.class, HiveStorageTest.class})
public class TestSqlStdBasedAuthorization extends BaseTestHiveImpersonation {

  private static final String db_general = "db_general";

  // Tables in "db_general"
  private static final String g_student_user0 = "student_user0";
  private static final String g_voter_role0 = "voter_role0";
  private static final String g_student_user2 = "student_user2";


  // Create a view on "g_student_user0". View is owned by user0:group0 and has permissions 750
  private static final String v_student_u0g0_750 = "v_student_u0g0_750";

  // Create a view on "v_student_u0g0_750". View is owned by user1:group1 and has permissions 750
  private static final String v_student_u1g1_750 = "v_student_u1g1_750";

  private static final String query_v_student_u0g0_750 = String.format(
      "SELECT rownum FROM %s.%s.%s ORDER BY rownum LIMIT 1", MINI_DFS_STORAGE_PLUGIN_NAME, "tmp", v_student_u0g0_750);

  private static final String query_v_student_u1g1_750 = String.format(
      "SELECT rownum FROM %s.%s.%s ORDER BY rownum LIMIT 1", MINI_DFS_STORAGE_PLUGIN_NAME, "tmp", v_student_u1g1_750);

  // Role for testing purpose
  private static final String test_role0 = "role0";

  @BeforeClass
  public static void setup() throws Exception {
    startMiniDfsCluster(TestSqlStdBasedAuthorization.class.getSimpleName());
    prepHiveConfAndData();
    setSqlStdBasedAuthorizationInHiveConf();
    startHiveMetaStore();
    startDrillCluster(true);
    addHiveStoragePlugin(getHivePluginConfig());
    addMiniDfsBasedStorage(new HashMap<>());
    generateTestData();
  }

  private static void setSqlStdBasedAuthorizationInHiveConf() {
    hiveConf.set(ConfVars.HIVE_AUTHORIZATION_ENABLED.varname, "true");
    hiveConf.set(HIVE_AUTHENTICATOR_MANAGER.varname, SessionStateConfigUserAuthenticator.class.getName());
    hiveConf.set(HIVE_AUTHORIZATION_MANAGER.varname, SQLStdConfOnlyAuthorizerFactory.class.getName());
    hiveConf.set(ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, "false");
    hiveConf.set(ConfVars.METASTORE_EXECUTE_SET_UGI.varname, "false");
    hiveConf.set(ConfVars.USERS_IN_ADMIN_ROLE.varname, processUser);
  }

  private static Map<String, String> getHivePluginConfig() {
    final Map<String, String> hiveConfig = Maps.newHashMap();
    hiveConfig.put(METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname));
    hiveConfig.put(FS_DEFAULT_NAME_KEY, dfsConf.get(FS_DEFAULT_NAME_KEY));
    hiveConfig.put(HIVE_SERVER2_ENABLE_DOAS.varname, hiveConf.get(HIVE_SERVER2_ENABLE_DOAS.varname));
    hiveConfig.put(METASTORE_EXECUTE_SET_UGI.varname, hiveConf.get(METASTORE_EXECUTE_SET_UGI.varname));
    hiveConfig.put(HIVE_AUTHORIZATION_ENABLED.varname, hiveConf.get(HIVE_AUTHORIZATION_ENABLED.varname));
    hiveConfig.put(HIVE_AUTHENTICATOR_MANAGER.varname, SessionStateUserAuthenticator.class.getName());
    hiveConfig.put(HIVE_AUTHORIZATION_MANAGER.varname, SQLStdHiveAuthorizerFactory.class.getName());
    hiveConfig.put(METASTORE_SCHEMA_VERIFICATION.varname, hiveConf.get(METASTORE_SCHEMA_VERIFICATION.varname));
    hiveConfig.put(METASTORE_AUTO_CREATE_ALL.varname, hiveConf.get(METASTORE_AUTO_CREATE_ALL.varname));
    hiveConfig.put(HIVE_CBO_ENABLED.varname, hiveConf.get(HIVE_CBO_ENABLED.varname));
    return hiveConfig;
  }

  private static void generateTestData() throws Exception {
    final SessionState ss = new SessionState(hiveConf);
    SessionState.start(ss);
    final Driver driver = new Driver(hiveConf);

    executeQuery(driver, "CREATE DATABASE " + db_general);
    createTbl(driver, db_general, g_student_user0, studentDef, studentData);
    createTbl(driver, db_general, g_voter_role0, voterDef, voterData);
    createTbl(driver, db_general, g_student_user2, studentDef, studentData);

    executeQuery(driver, "SET ROLE admin");
    executeQuery(driver, "CREATE ROLE " + test_role0);
    executeQuery(driver, "GRANT ROLE " + test_role0 + " TO USER " + org1Users[1]);
    executeQuery(driver, "GRANT ROLE " + test_role0 + " TO USER " + org1Users[2]);

    executeQuery(driver, String.format("GRANT SELECT ON %s.%s TO USER %s", db_general, g_student_user0, org1Users[0]));
    executeQuery(driver, String.format("GRANT SELECT ON %s.%s TO ROLE %s", db_general, g_voter_role0, test_role0));
    executeQuery(driver, String.format("GRANT SELECT ON %s.%s TO USER %s", db_general, g_student_user2, org1Users[2]));

    createView(org1Users[0], org1Groups[0], v_student_u0g0_750,
        String.format("SELECT rownum, name, age, studentnum FROM %s.%s.%s",
            hivePluginName, db_general, g_student_user0));

    createView(org1Users[1], org1Groups[1], v_student_u1g1_750,
        String.format("SELECT rownum, name, age FROM %s.%s.%s", MINI_DFS_STORAGE_PLUGIN_NAME, "tmp", v_student_u0g0_750));
  }

  private static void createTbl(final Driver driver, final String db, final String tbl, final String tblDef,
      final String data) throws Exception {
    executeQuery(driver, String.format(tblDef, db, tbl));
    executeQuery(driver, String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE %s.%s", data, db, tbl));
  }

  // Irrespective of each db permissions, all dbs show up in "SHOW SCHEMAS"
  @Test
  public void showSchemas() throws Exception {
    testBuilder()
        .sqlQuery("SHOW SCHEMAS LIKE 'hive.%'")
        .unOrdered()
        .baselineColumns("SCHEMA_NAME")
        .baselineValues("hive.db_general")
        .baselineValues("hive.default")
        .go();
  }

  @Test
  public void showTables_user0() throws Exception {
    updateClient(org1Users[0]);
    showTablesHelper(db_general,
        // Users are expected to see all tables in a database even if they don't have permissions to read from tables.
        ImmutableList.of(
            g_student_user0,
            g_student_user2,
            g_voter_role0
        ));
  }

  @Test
  public void showTables_user1() throws Exception {
    updateClient(org1Users[1]);
    showTablesHelper(db_general,
        // Users are expected to see all tables in a database even if they don't have permissions to read from tables.
        ImmutableList.of(
            g_student_user0,
            g_student_user2,
            g_voter_role0
        ));
  }

  @Test
  public void select_user0_1() throws Exception {
    // SELECT on "student_user0" table is granted to user "user0"
    updateClient(org1Users[0]);
    test("USE " + hivePluginName + "." + db_general);
    test(String.format("SELECT * FROM %s ORDER BY name LIMIT 2", g_student_user0));
  }

  @Test
  public void select_user0_2() throws Exception {
    // SELECT on table "student_user0" is NOT granted to user "user0" directly or indirectly through role "role0" as
    // user "user0" is not part of role "role0"
    updateClient(org1Users[0]);
    test("USE " + hivePluginName + "." + db_general);
    final String query = String.format("SELECT * FROM %s ORDER BY name LIMIT 2", g_voter_role0);
    errorMsgTestHelper(query, "Principal [name=user0_1, type=USER] does not have following privileges for " +
        "operation QUERY [[SELECT] on Object [type=TABLE_OR_VIEW, name=db_general.voter_role0]]\n");
  }

  @Test
  public void select_user1_1() throws Exception {
    // SELECT on table "student_user0" is NOT granted to user "user1"
    updateClient(org1Users[1]);
    test("USE " + hivePluginName + "." + db_general);
    final String query = String.format("SELECT * FROM %s ORDER BY name LIMIT 2", g_student_user0);
    errorMsgTestHelper(query, "Principal [name=user1_1, type=USER] does not have following privileges for " +
        "operation QUERY [[SELECT] on Object [type=TABLE_OR_VIEW, name=db_general.student_user0]]\n");
  }

  @Test
  public void select_user1_2() throws Exception {
    // SELECT on "voter_role0" table is granted to role "role0" and user "user1" is part the role "role0"
    updateClient(org1Users[1]);
    test("USE " + hivePluginName + "." + db_general);
    test(String.format("SELECT * FROM %s ORDER BY name LIMIT 2", g_voter_role0));
  }

  @Test
  public void select_user1_3() throws Exception {
    // SELECT on "voter_role0" table is granted to role "role0" and user "user1" is part the role "role0"
    // SELECT on "student_user2" table is NOT granted to either role "role0" or user "user1"
    updateClient(org1Users[1]);
    test("USE " + hivePluginName + "." + db_general);
    final String query =
        String.format("SELECT * FROM %s v JOIN %s s on v.name = s.name limit 2;", g_voter_role0, g_student_user2);
    errorMsgTestHelper(query, "Principal [name=user1_1, type=USER] does not have following privileges for " +
        "operation QUERY [[SELECT] on Object [type=TABLE_OR_VIEW, name=db_general.student_user2]]");
  }

  @Test
  public void select_user2_1() throws Exception {
    // SELECT on "voter_role0" table is granted to role "role0" and user "user2" is part the role "role0"
    updateClient(org1Users[2]);
    test("USE " + hivePluginName + "." + db_general);
    test(String.format("SELECT * FROM %s ORDER BY name LIMIT 2", g_voter_role0));
  }

  @Test
  public void select_user2_2() throws Exception {
    // SELECT on "student_user2" table is granted to user "user2"
    updateClient(org1Users[2]);
    test("USE " + hivePluginName + "." + db_general);
    test(String.format("SELECT * FROM %s ORDER BY name LIMIT 2", g_student_user2));
  }

  @Test
  public void select_user2_3() throws Exception {
    // SELECT on "voter_role0" table is granted to role "role0" and user "user2" is part the role "role0"
    // SELECT on "student_user2" table is granted to user "user2"
    updateClient(org1Users[2]);
    test("USE " + hivePluginName + "." + db_general);
    test(String.format("SELECT * FROM %s v JOIN %s s on v.name = s.name limit 2;", g_voter_role0, g_student_user2));
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

  @AfterClass
  public static void shutdown() throws Exception {
    stopMiniDfsCluster();
    stopHiveMetaStore();
  }
}
