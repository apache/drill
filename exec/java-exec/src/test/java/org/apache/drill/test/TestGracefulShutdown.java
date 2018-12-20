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
package org.apache.drill.test;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.Drillbit;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@Category({SlowTest.class})
public class TestGracefulShutdown extends BaseTestQuery {

  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule(120_000);

  @BeforeClass
  public static void setUpTestData() throws Exception {
    for( int i = 0; i < 300; i++) {
      setupFile(i);
    }
  }

  private static void enableWebServer(ClusterFixtureBuilder builder) {
    enableDrillPortHunting(builder);
    builder.configBuilder.put(ExecConstants.HTTP_ENABLE, true);
    builder.configBuilder.put(ExecConstants.HTTP_PORT_HUNT, true);
    builder.sessionOption(ExecConstants.SLICE_TARGET, 10);
  }

  private static void enableDrillPortHunting(ClusterFixtureBuilder builder) {
    builder.configBuilder.put(ExecConstants.DRILL_PORT_HUNT, true);
    builder.configBuilder.put(ExecConstants.GRACE_PERIOD, 500);
    builder.configBuilder.put(ExecConstants.ALLOW_LOOPBACK_ADDRESS_BINDING, true);
  }

  /*
  Start multiple drillbits and then shutdown a drillbit. Query the online
  endpoints and check if the drillbit still exists.
   */
  @Test
  public void testOnlineEndPoints() throws  Exception {

    String[] drillbits = {"db1", "db2", "db3"};
    ClusterFixtureBuilder builder = ClusterFixture.bareBuilder(dirTestWatcher).withLocalZk().withBits(drillbits);
    enableDrillPortHunting(builder);

    try ( ClusterFixture cluster = builder.build()) {

      Drillbit drillbit = cluster.drillbit("db2");
      int zkRefresh = drillbit.getContext().getConfig().getInt(ExecConstants.ZK_REFRESH);
      DrillbitEndpoint drillbitEndpoint =  drillbit.getRegistrationHandle().getEndPoint();
      cluster.closeDrillbit("db2");

      while (true) {
        Collection<DrillbitEndpoint> drillbitEndpoints = cluster.drillbit()
                .getContext()
                .getClusterCoordinator()
                .getOnlineEndPoints();

        if (!drillbitEndpoints.contains(drillbitEndpoint)) {
          // Success
          return;
        }

        Thread.sleep(zkRefresh);
      }
    }
  }

  /*
   Test shutdown through RestApi
   */
  @Test
  public void testRestApi() throws Exception {

    String[] drillbits = {"db1", "db2", "db3"};
    ClusterFixtureBuilder builder = ClusterFixture.bareBuilder(dirTestWatcher).withLocalZk().withBits(drillbits);
    enableWebServer(builder);
    QueryBuilder.QuerySummaryFuture listener;
    final String sql = "select * from dfs.root.`.`";
    try (ClusterFixture cluster = builder.build();
         final ClientFixture client = cluster.clientFixture()) {
      Drillbit drillbit = cluster.drillbit("db1");
      int port = drillbit.getWebServerPort();
      int zkRefresh = drillbit.getContext().getConfig().getInt(ExecConstants.ZK_REFRESH);
      listener = client.queryBuilder().sql(sql).futureSummary();
      URL url = new URL("http://localhost:" + port + "/gracefulShutdown");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("POST");
      if (conn.getResponseCode() != 200) {
        throw new RuntimeException("Failed : HTTP error code : "
                + conn.getResponseCode());
      }
      while (true) {
        if (listener.isDone()) {
          break;
        }
        Thread.sleep(100L);
      }

      if (waitAndAssertDrillbitCount(cluster, zkRefresh)) {
        return;
      }
      Assert.fail("Timed out");
    }
  }

  /*
   Test default shutdown through RestApi
   */
  @Test
  public void testRestApiShutdown() throws Exception {

    String[] drillbits = {"db1", "db2", "db3"};
    ClusterFixtureBuilder builder = ClusterFixture.bareBuilder(dirTestWatcher).withLocalZk().withBits(drillbits);
    enableWebServer(builder);
    QueryBuilder.QuerySummaryFuture listener;
    final String sql = "select * from dfs.root.`.`";
    try (ClusterFixture cluster = builder.build();
         final ClientFixture client = cluster.clientFixture()) {
      Drillbit drillbit = cluster.drillbit("db1");
      int port = drillbit.getWebServerPort();
      int zkRefresh = drillbit.getContext().getConfig().getInt(ExecConstants.ZK_REFRESH);
      listener =  client.queryBuilder().sql(sql).futureSummary();
      while (true) {
        if (listener.isDone()) {
          break;
        }

        Thread.sleep(100L);
      }
      URL url = new URL("http://localhost:" + port + "/shutdown");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("POST");
      if (conn.getResponseCode() != 200) {
        throw new RuntimeException("Failed : HTTP error code : "
                + conn.getResponseCode());
      }
      if (waitAndAssertDrillbitCount(cluster, zkRefresh)) {
        return;
      }
      Assert.fail("Timed out");
    }
  }

  @Test // DRILL-6912
  public void gracefulShutdownThreadShouldBeInitializedBeforeClosingDrillbit() throws Exception {
    Drillbit drillbit = null;
    Drillbit drillbitWithSamePort = null;

    int userPort = QueryTestUtil.getFreePortNumber(31170, 300);
    int bitPort = QueryTestUtil.getFreePortNumber(31180, 300);
    ClusterFixtureBuilder fixtureBuilder = ClusterFixture.bareBuilder(dirTestWatcher).withLocalZk()
        .configProperty(ExecConstants.INITIAL_USER_PORT, userPort)
        .configProperty(ExecConstants.INITIAL_BIT_PORT, bitPort);
    try (ClusterFixture clusterFixture = fixtureBuilder.build()) {
      drillbit = clusterFixture.drillbit();

      // creating another drillbit instance using same config
      drillbitWithSamePort = new Drillbit(clusterFixture.config(), fixtureBuilder.configBuilder().getDefinitions(),
          clusterFixture.serviceSet());

      try {
        drillbitWithSamePort.run();
        fail("drillbitWithSamePort.run() should throw UserException");
      } catch (UserException e) {
        // it's expected that second drillbit can't be started because port is busy
        assertThat(e.getMessage(), containsString("RESOURCE ERROR: Drillbit could not bind to port"));
      }
    } finally {
      // preconditions
      assertNotNull(drillbit);
      assertNotNull(drillbitWithSamePort);
      assertNotNull("gracefulShutdownThread should be initialized, otherwise NPE will be thrown from close()",
          drillbit.getGracefulShutdownThread());
      // main test case
      assertNotNull("gracefulShutdownThread should be initialized, otherwise NPE will be thrown from close()",
          drillbitWithSamePort.getGracefulShutdownThread());
      drillbit.close();
      drillbitWithSamePort.close();
    }
  }

  private static boolean waitAndAssertDrillbitCount(ClusterFixture cluster, int zkRefresh) throws InterruptedException {

    while (true) {
      Collection<DrillbitEndpoint> drillbitEndpoints = cluster.drillbit()
              .getContext()
              .getClusterCoordinator()
              .getAvailableEndpoints();
      if (drillbitEndpoints.size() == 2) {
        return true;
      }

      Thread.sleep(zkRefresh);
    }
  }

  private static void setupFile(int file_num) throws Exception {
    final String file = "employee"+file_num+".json";
    final Path path = dirTestWatcher.getRootDir().toPath().resolve(file);
    try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(path.toFile(), true)))) {
      out.println("{\"employee_id\":1,\"full_name\":\"Sheri Nowmer\",\"first_name\":\"Sheri\",\"last_name\":\"Nowmer\",\"position_id\":1,\"position_title\":\"President\",\"store_id\":0,\"department_id\":1,\"birth_date\":\"1961-08-26\",\"hire_date\":\"1994-12-01 00:00:00.0\",\"end_date\":null,\"salary\":80000.0000,\"supervisor_id\":0,\"education_level\":\"Graduate Degree\",\"marital_status\":\"S\",\"gender\":\"F\",\"management_role\":\"Senior Management\"}\n" +
              "{\"employee_id\":2,\"full_name\":\"Derrick Whelply\",\"first_name\":\"Derrick\",\"last_name\":\"Whelply\",\"position_id\":2,\"position_title\":\"VP Country Manager\",\"store_id\":0,\"department_id\":1,\"birth_date\":\"1915-07-03\",\"hire_date\":\"1994-12-01 00:00:00.0\",\"end_date\":null,\"salary\":40000.0000,\"supervisor_id\":1,\"education_level\":\"Graduate Degree\",\"marital_status\":\"M\",\"gender\":\"M\",\"management_role\":\"Senior Management\"}\n" +
              "{\"employee_id\":4,\"full_name\":\"Michael Spence\",\"first_name\":\"Michael\",\"last_name\":\"Spence\",\"position_id\":2,\"position_title\":\"VP Country Manager\",\"store_id\":0,\"department_id\":1,\"birth_date\":\"1969-06-20\",\"hire_date\":\"1998-01-01 00:00:00.0\",\"end_date\":null,\"salary\":40000.0000,\"supervisor_id\":1,\"education_level\":\"Graduate Degree\",\"marital_status\":\"S\",\"gender\":\"M\",\"management_role\":\"Senior Management\"}\n" +
              "{\"employee_id\":5,\"full_name\":\"Maya Gutierrez\",\"first_name\":\"Maya\",\"last_name\":\"Gutierrez\",\"position_id\":2,\"position_title\":\"VP Country Manager\",\"store_id\":0,\"department_id\":1,\"birth_date\":\"1951-05-10\",\"hire_date\":\"1998-01-01 00:00:00.0\",\"end_date\":null,\"salary\":35000.0000,\"supervisor_id\":1,\"education_level\":\"Bachelors Degree\",\"marital_status\":\"M\",\"gender\":\"F\",\"management_role\":\"Senior Management\"}\n" +
              "{\"employee_id\":6,\"full_name\":\"Roberta Damstra\",\"first_name\":\"Roberta\",\"last_name\":\"Damstra\",\"position_id\":3,\"position_title\":\"VP Information Systems\",\"store_id\":0,\"department_id\":2,\"birth_date\":\"1942-10-08\",\"hire_date\":\"1994-12-01 00:00:00.0\",\"end_date\":null,\"salary\":25000.0000,\"supervisor_id\":1,\"education_level\":\"Bachelors Degree\",\"marital_status\":\"M\",\"gender\":\"F\",\"management_role\":\"Senior Management\"}\n" +
              "{\"employee_id\":7,\"full_name\":\"Rebecca Kanagaki\",\"first_name\":\"Rebecca\",\"last_name\":\"Kanagaki\",\"position_id\":4,\"position_title\":\"VP Human Resources\",\"store_id\":0,\"department_id\":3,\"birth_date\":\"1949-03-27\",\"hire_date\":\"1994-12-01 00:00:00.0\",\"end_date\":null,\"salary\":15000.0000,\"supervisor_id\":1,\"education_level\":\"Bachelors Degree\",\"marital_status\":\"M\",\"gender\":\"F\",\"management_role\":\"Senior Management\"}\n");
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }
}
