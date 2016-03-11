/**
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
package com.mapr.drill.maprdb.tests;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.hbase.HBaseTestsSuite;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.json.Json;

import com.mapr.db.Admin;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import com.mapr.drill.maprdb.tests.binary.TestMapRDBFilterPushDown;
import com.mapr.drill.maprdb.tests.binary.TestMapRDBSimple;
import com.mapr.drill.maprdb.tests.json.TestSimpleJson;

@RunWith(Suite.class)
@SuiteClasses({
  TestMapRDBSimple.class,
  TestMapRDBFilterPushDown.class,
  TestSimpleJson.class
})
public class MaprDBTestsSuite {
  private static final String TMP_BUSINESS_TABLE = "/tmp/business";

  private static final boolean IS_DEBUG = ManagementFactory.getRuntimeMXBean().getInputArguments().toString().indexOf("-agentlib:jdwp") > 0;

  private static volatile AtomicInteger initCount = new AtomicInteger(0);
  private static volatile Configuration conf;

  private static Admin admin;

  @BeforeClass
  public static void setupTests() throws Exception {
    if (initCount.get() == 0) {
      synchronized (MaprDBTestsSuite.class) {
        if (initCount.get() == 0) {
          HBaseTestsSuite.configure(false /*manageHBaseCluster*/, true /*createTables*/);
          HBaseTestsSuite.initCluster();
          createJsonTables();

          // Sleep to allow table data to be flushed to tables.
          // Without this, the row count stats to return 0,
          // causing the planner to reject optimized plans.
          System.out.println("Sleeping for 5 seconds to allow table flushes");
          Thread.sleep(5000);

          conf = HBaseTestsSuite.getConf();
          initCount.incrementAndGet(); // must increment while inside the synchronized block
          return;
        }
      }
    }
    initCount.incrementAndGet();
    return;
  }

  @AfterClass
  public static void cleanupTests() throws Exception {
    synchronized (MaprDBTestsSuite.class) {
      if (initCount.decrementAndGet() == 0) {
        HBaseTestsSuite.tearDownCluster();
        deleteJsonTables();
      }
    }
  }

  private static volatile boolean pluginCreated;

  public static Configuration createPluginAndGetConf(DrillbitContext ctx) throws Exception {
    if (!pluginCreated) {
      synchronized (MaprDBTestsSuite.class) {
        if (!pluginCreated) {
          String pluginConfStr = "{" +
              "  \"type\": \"file\"," +
              "  \"enabled\": true," +
              "  \"connection\": \"maprfs:///\"," +
              "  \"workspaces\": {" +
              "    \"default\": {" +
              "      \"location\": \"/tmp\"," +
              "      \"writable\": false," +
              "      \"defaultInputFormat\": \"maprdb\"" +
              "    }," +
              "    \"root\": {" +
              "      \"location\": \"/\"," +
              "      \"writable\": false," +
              "      \"defaultInputFormat\": \"maprdb\"" +
              "    }" +
              "  }," +
              "  \"formats\": {" +
              "   \"maprdb\": {" +
              "      \"type\": \"maprdb\"," +
              "      \"allTextMode\": false," +
              "      \"readAllNumbersAsDouble\": false," +
              "      \"enablePushdown\": true" +
              "    }," +
              "   \"streams\": {" +
              "      \"type\": \"streams\"" +
              "    }" +
              "  }" +
              "}";

          FileSystemConfig pluginConfig = ctx.getLpPersistence().getMapper().readValue(pluginConfStr, FileSystemConfig.class);
          // create the plugin with "hbase" name so that we can run HBase unit tests against them
          ctx.getStorage().createOrUpdate("hbase", pluginConfig, true);
        }
      }
    }
    return conf;
  }

  public static boolean isDebug() {
    return IS_DEBUG;
  }

  public static InputStream getJsonStream(String resourceName) {
    return MaprDBTestsSuite.class.getClassLoader().getResourceAsStream(resourceName);
  }

  public static void createJsonTables() throws IOException {
    admin = MapRDB.newAdmin();
    if (admin.tableExists(TMP_BUSINESS_TABLE)) {
      admin.deleteTable(TMP_BUSINESS_TABLE);
    }

    try (Table table = admin.createTable(TMP_BUSINESS_TABLE);
         InputStream in = getJsonStream("json/business.json");
         DocumentStream stream = Json.newDocumentStream(in)) {
      for (Document document : stream) {
        table.insert(document, "business_id");
      }
      table.flush();
    }
  }

  public static void deleteJsonTables() {
    if (admin != null) {
      if (admin.tableExists(TMP_BUSINESS_TABLE)) {
        admin.deleteTable(TMP_BUSINESS_TABLE);
      }
      admin.close();
    }
  }

}
