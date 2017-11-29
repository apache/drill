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
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.Drillbit;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collection;
import java.util.Properties;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;
import java.nio.file.Path;
import java.io.BufferedWriter;



@Category({SlowTest.class})
public class TestGracefulShutdown extends BaseTestQuery{

  @BeforeClass
  public static void setUpTestData() throws Exception {

    for( int i = 0; i < 300; i++) {
      setupFile(i);
    }
  }


  public static final Properties WEBSERVER_CONFIGURATION = new Properties() {
    {
      put(ExecConstants.HTTP_ENABLE, true);
      put(ExecConstants.HTTP_PORT_HUNT, true);
      put(ExecConstants.DRILL_PORT_HUNT, true);
      put(ExecConstants.GRACE_PERIOD, 10000);
    }
  };

  public static final Properties DRILL_PORT_CONFIGURATION = new Properties() {
    {
      put(ExecConstants.DRILL_PORT_HUNT, true);
      put(ExecConstants.GRACE_PERIOD, 10000);
    }
  };

  public ClusterFixtureBuilder enableWebServer(ClusterFixtureBuilder builder) {
    Properties props = new Properties();
    props.putAll(WEBSERVER_CONFIGURATION);
    builder.configBuilder.configProps(props);
    builder.sessionOption(ExecConstants.SLICE_TARGET, 10);
    return builder;
  }

  public ClusterFixtureBuilder enableDrillPortHunting(ClusterFixtureBuilder builder) {
    Properties props = new Properties();
    props.putAll(DRILL_PORT_CONFIGURATION);
    builder.configBuilder.configProps(props);
    return builder;
  }



  /*
  Start multiple drillbits and then shutdown a drillbit. Query the online
  endpoints and check if the drillbit still exists.
   */
  @Test
  public void testOnlineEndPoints() throws  Exception {

    String[] drillbits = {"db1" ,"db2","db3", "db4", "db5", "db6"};
    ClusterFixtureBuilder builder = ClusterFixture.bareBuilder(dirTestWatcher).withBits(drillbits).withLocalZk();
    enableDrillPortHunting(builder);

    try ( ClusterFixture cluster = builder.build();
          ClientFixture client = cluster.clientFixture()) {

      Drillbit drillbit = cluster.drillbit("db2");
      DrillbitEndpoint drillbitEndpoint =  drillbit.getRegistrationHandle().getEndPoint();
      int grace_period = drillbit.getContext().getConfig().getInt(ExecConstants.GRACE_PERIOD);
      new Thread(new Runnable() {
        public void run() {
          try {
            cluster.closeDrillbit("db2");
          } catch (Exception e) {
            fail();
          }
        }
      }).start();
      //wait for graceperiod
      Thread.sleep(grace_period);
      Collection<DrillbitEndpoint> drillbitEndpoints = cluster.drillbit().getContext()
              .getClusterCoordinator()
              .getOnlineEndPoints();
      Assert.assertFalse(drillbitEndpoints.contains(drillbitEndpoint));
    }
  }
  /*
    Test if the drillbit transitions from ONLINE state when a shutdown
    request is initiated
   */
  @Test
  public void testStateChange() throws  Exception {

    String[] drillbits = {"db1" ,"db2", "db3", "db4", "db5", "db6"};
    ClusterFixtureBuilder builder = ClusterFixture.bareBuilder(dirTestWatcher).withBits(drillbits).withLocalZk();
    enableDrillPortHunting(builder);

    try ( ClusterFixture cluster = builder.build();
          ClientFixture client = cluster.clientFixture()) {
      Drillbit drillbit = cluster.drillbit("db2");
      int grace_period = drillbit.getContext().getConfig().getInt(ExecConstants.GRACE_PERIOD);
      DrillbitEndpoint drillbitEndpoint =  drillbit.getRegistrationHandle().getEndPoint();
      new Thread(new Runnable() {
        public void run() {
          try {
            cluster.closeDrillbit("db2");
          } catch (Exception e) {
            fail();
          }
        }
      }).start();
      Thread.sleep(grace_period);
      Collection<DrillbitEndpoint> drillbitEndpoints = cluster.drillbit().getContext()
              .getClusterCoordinator()
              .getAvailableEndpoints();
      for (DrillbitEndpoint dbEndpoint : drillbitEndpoints) {
        if(drillbitEndpoint.getAddress().equals(dbEndpoint.getAddress()) && drillbitEndpoint.getUserPort() == dbEndpoint.getUserPort()) {
          assertNotEquals(dbEndpoint.getState(),DrillbitEndpoint.State.ONLINE);
        }
      }
    }
  }

  /*
   Test shutdown through RestApi
   */
  @Test
  public void testRestApi() throws Exception {

    String[] drillbits = { "db1" ,"db2", "db3" };
    ClusterFixtureBuilder builder = ClusterFixture.bareBuilder(dirTestWatcher).withBits(drillbits).withLocalZk();
    builder = enableWebServer(builder);
    QueryBuilder.QuerySummaryFuture listener;
    final String sql = "select * from dfs.root.`.`";
    try ( ClusterFixture cluster = builder.build();
          final ClientFixture client = cluster.clientFixture()) {
      Drillbit drillbit = cluster.drillbit("db1");
      int port = drillbit.getContext().getConfig().getInt("drill.exec.http.port");
      int grace_period = drillbit.getContext().getConfig().getInt(ExecConstants.GRACE_PERIOD);
      listener =  client.queryBuilder().sql(sql).futureSummary();
      Thread.sleep(60000);
      while( port < 8049) {
        URL url = new URL("http://localhost:"+port+"/gracefulShutdown");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        if (conn.getResponseCode() != 200) {
          throw new RuntimeException("Failed : HTTP error code : "
                  + conn.getResponseCode());
        }
        port++;
      }
      Thread.sleep(grace_period);
      Collection<DrillbitEndpoint> drillbitEndpoints = cluster.drillbit().getContext()
              .getClusterCoordinator()
              .getOnlineEndPoints();
      while(!listener.isDone()) {
        Thread.sleep(10);
      }
      Assert.assertTrue(listener.isDone());
      Assert.assertEquals(1,drillbitEndpoints.size());
    }
  }

  /*
   Test default shutdown through RestApi
   */
  @Test
  public void testRestApiShutdown() throws Exception {

    String[] drillbits = {"db1" ,"db2", "db3"};
    ClusterFixtureBuilder builder = ClusterFixture.bareBuilder(dirTestWatcher).withBits(drillbits).withLocalZk();
    builder = enableWebServer(builder);
    QueryBuilder.QuerySummaryFuture listener;
    final String sql = "select * from dfs.root.`.`";
    try ( ClusterFixture cluster = builder.build();
          final ClientFixture client = cluster.clientFixture()) {
      Drillbit drillbit = cluster.drillbit("db1");
      int port = drillbit.getContext().getConfig().getInt("drill.exec.http.port");
      int grace_period = drillbit.getContext().getConfig().getInt(ExecConstants.GRACE_PERIOD);
      listener =  client.queryBuilder().sql(sql).futureSummary();
      Thread.sleep(10000);
      while( port < 8048) {
        URL url = new URL("http://localhost:"+port+"/shutdown");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        if (conn.getResponseCode() != 200) {
          throw new RuntimeException("Failed : HTTP error code : "
                  + conn.getResponseCode());
        }
        port++;
      }
      Thread.sleep(grace_period);
      Thread.sleep(5000);
      Collection<DrillbitEndpoint> drillbitEndpoints = cluster.drillbit().getContext()
              .getClusterCoordinator().getAvailableEndpoints();
      while(!listener.isDone()) {
        Thread.sleep(10);
      }
      Assert.assertTrue(listener.isDone());
      Assert.assertEquals(2,drillbitEndpoints.size());
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
