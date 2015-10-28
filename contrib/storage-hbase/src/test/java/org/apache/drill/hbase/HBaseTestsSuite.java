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
package org.apache.drill.hbase;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
  TestHBaseQueries.class,
  TestHBaseRegexParser.class,
  HBaseRecordReaderTest.class,
  TestHBaseFilterPushDown.class,
  TestHBaseProjectPushDown.class,
  TestHBaseRegionScanAssignments.class,
  TestHBaseTableProvider.class,
  TestHBaseCFAsJSONString.class
})
public class HBaseTestsSuite {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseTestsSuite.class);

  private static final boolean IS_DEBUG = ManagementFactory.getRuntimeMXBean().getInputArguments().toString().indexOf("-agentlib:jdwp") > 0;

  protected static final String TEST_TABLE_1 = "TestTable1";
  protected static final String TEST_TABLE_3 = "TestTable3";
  protected static final String TEST_TABLE_COMPOSITE_DATE = "TestTableCompositeDate";
  protected static final String TEST_TABLE_COMPOSITE_TIME = "TestTableCompositeTime";
  protected static final String TEST_TABLE_COMPOSITE_INT = "TestTableCompositeInt";
  protected static final String TEST_TABLE_DOUBLE_OB = "TestTableDoubleOB";
  protected static final String TEST_TABLE_FLOAT_OB = "TestTableFloatOB";
  protected static final String TEST_TABLE_BIGINT_OB = "TestTableBigIntOB";
  protected static final String TEST_TABLE_INT_OB = "TestTableIntOB";
  protected static final String TEST_TABLE_DOUBLE_OB_DESC = "TestTableDoubleOBDesc";
  protected static final String TEST_TABLE_FLOAT_OB_DESC = "TestTableFloatOBDesc";
  protected static final String TEST_TABLE_BIGINT_OB_DESC = "TestTableBigIntOBDesc";
  protected static final String TEST_TABLE_INT_OB_DESC = "TestTableIntOBDesc";

  private static Configuration conf;

  private static HBaseAdmin admin;

  private static HBaseTestingUtility UTIL;

  private static volatile AtomicInteger initCount = new AtomicInteger(0);

  /**
   * This flag controls whether {@link HBaseTestsSuite} starts a mini HBase cluster to run the unit test.
   */
  private static boolean manageHBaseCluster = System.getProperty("drill.hbase.tests.manageHBaseCluster", "true").equalsIgnoreCase("true");
  private static boolean hbaseClusterCreated = false;

  private static boolean createTables = System.getProperty("drill.hbase.tests.createTables", "true").equalsIgnoreCase("true");
  private static boolean tablesCreated = false;

  @BeforeClass
  public static void initCluster() throws Exception {
    if (initCount.get() == 0) {
      synchronized (HBaseTestsSuite.class) {
        if (initCount.get() == 0) {
          conf = HBaseConfiguration.create();
          conf.set(HConstants.HBASE_CLIENT_INSTANCE_ID, "drill-hbase-unit-tests-client");
          if (IS_DEBUG) {
            conf.set("hbase.regionserver.lease.period","10000000");
          }

          if (manageHBaseCluster) {
            logger.info("Starting HBase mini cluster.");
            UTIL = new HBaseTestingUtility(conf);
            UTIL.startMiniZKCluster();
            String old_home = System.getProperty("user.home");
            System.setProperty("user.home", UTIL.getDataTestDir().toString());
            UTIL.startMiniHBaseCluster(1, 1);
            System.setProperty("user.home", old_home);
            hbaseClusterCreated = true;
            logger.info("HBase mini cluster started. Zookeeper port: '{}'", getZookeeperPort());
          }

          admin = new HBaseAdmin(conf);

          if (createTables || !tablesExist()) {
            createTestTables();
            tablesCreated = true;
          }
          initCount.incrementAndGet();
          return;
        }
      }
    }
    initCount.incrementAndGet();
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    synchronized (HBaseTestsSuite.class) {
      if (initCount.decrementAndGet() == 0) {
        if (createTables && tablesCreated) {
          cleanupTestTables();
        }

        if (admin != null) {
          admin.close();
        }

        if (hbaseClusterCreated) {
          logger.info("Shutting down HBase mini cluster.");
          UTIL.shutdownMiniCluster();
          logger.info("HBase mini cluster stopped.");
        }
      }
    }
  }

  public static Configuration getConf() {
    return conf;
  }

  public static HBaseTestingUtility getHBaseTestingUtility() {
    return UTIL;
  }

  private static boolean tablesExist() throws IOException {
    return admin.tableExists(TEST_TABLE_1) && admin.tableExists(TEST_TABLE_3)
           && admin.tableExists(TEST_TABLE_COMPOSITE_DATE)
           && admin.tableExists(TEST_TABLE_COMPOSITE_TIME)
           && admin.tableExists(TEST_TABLE_COMPOSITE_INT)
           && admin.tableExists(TEST_TABLE_DOUBLE_OB)
           && admin.tableExists(TEST_TABLE_FLOAT_OB)
           && admin.tableExists(TEST_TABLE_BIGINT_OB)
           && admin.tableExists(TEST_TABLE_INT_OB)
           && admin.tableExists(TEST_TABLE_DOUBLE_OB_DESC)
           && admin.tableExists(TEST_TABLE_FLOAT_OB_DESC)
           && admin.tableExists(TEST_TABLE_BIGINT_OB_DESC)
           && admin.tableExists(TEST_TABLE_INT_OB_DESC);
  }

  private static void createTestTables() throws Exception {
    // TODO(DRILL-3954):  Change number of regions from 1 to multiple for other
    // tables and remaining problems not addressed by DRILL-2288 fixes.
    /*
     * We are seeing some issues with (Drill) Filter operator if a group scan span
     * multiple fragments. Hence the number of regions in the HBase table is set to 1.
     * Will revert to multiple region once the issue is resolved.
     */
    TestTableGenerator.generateHBaseDataset1(admin, TEST_TABLE_1, 2);
    TestTableGenerator.generateHBaseDataset3(admin, TEST_TABLE_3, 1);
    TestTableGenerator.generateHBaseDatasetCompositeKeyDate(admin, TEST_TABLE_COMPOSITE_DATE, 1);
    TestTableGenerator.generateHBaseDatasetCompositeKeyTime(admin, TEST_TABLE_COMPOSITE_TIME, 1);
    TestTableGenerator.generateHBaseDatasetCompositeKeyInt(admin, TEST_TABLE_COMPOSITE_INT, 1);
    TestTableGenerator.generateHBaseDatasetDoubleOB(admin, TEST_TABLE_DOUBLE_OB, 1);
    TestTableGenerator.generateHBaseDatasetFloatOB(admin, TEST_TABLE_FLOAT_OB, 1);
    TestTableGenerator.generateHBaseDatasetBigIntOB(admin, TEST_TABLE_BIGINT_OB, 1);
    TestTableGenerator.generateHBaseDatasetIntOB(admin, TEST_TABLE_INT_OB, 1);
    TestTableGenerator.generateHBaseDatasetDoubleOBDesc(admin, TEST_TABLE_DOUBLE_OB_DESC, 1);
    TestTableGenerator.generateHBaseDatasetFloatOBDesc(admin, TEST_TABLE_FLOAT_OB_DESC, 1);
    TestTableGenerator.generateHBaseDatasetBigIntOBDesc(admin, TEST_TABLE_BIGINT_OB_DESC, 1);
    TestTableGenerator.generateHBaseDatasetIntOBDesc(admin, TEST_TABLE_INT_OB_DESC, 1);
  }

  private static void cleanupTestTables() throws IOException {
    admin.disableTable(TEST_TABLE_1);
    admin.deleteTable(TEST_TABLE_1);
    admin.disableTable(TEST_TABLE_3);
    admin.deleteTable(TEST_TABLE_3);
    admin.disableTable(TEST_TABLE_COMPOSITE_DATE);
    admin.deleteTable(TEST_TABLE_COMPOSITE_DATE);
    admin.disableTable(TEST_TABLE_COMPOSITE_TIME);
    admin.deleteTable(TEST_TABLE_COMPOSITE_TIME);
    admin.disableTable(TEST_TABLE_COMPOSITE_INT);
    admin.deleteTable(TEST_TABLE_COMPOSITE_INT);
    admin.disableTable(TEST_TABLE_DOUBLE_OB);
    admin.deleteTable(TEST_TABLE_DOUBLE_OB);
    admin.disableTable(TEST_TABLE_FLOAT_OB);
    admin.deleteTable(TEST_TABLE_FLOAT_OB);
    admin.disableTable(TEST_TABLE_BIGINT_OB);
    admin.deleteTable(TEST_TABLE_BIGINT_OB);
    admin.disableTable(TEST_TABLE_INT_OB);
    admin.deleteTable(TEST_TABLE_INT_OB);
    admin.disableTable(TEST_TABLE_DOUBLE_OB_DESC);
    admin.deleteTable(TEST_TABLE_DOUBLE_OB_DESC);
    admin.disableTable(TEST_TABLE_FLOAT_OB_DESC);
    admin.deleteTable(TEST_TABLE_FLOAT_OB_DESC);
    admin.disableTable(TEST_TABLE_BIGINT_OB_DESC);
    admin.deleteTable(TEST_TABLE_BIGINT_OB_DESC);
    admin.disableTable(TEST_TABLE_INT_OB_DESC);
    admin.deleteTable(TEST_TABLE_INT_OB_DESC);
  }

  public static int getZookeeperPort() {
    return getConf().getInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181);
  }

  public static void configure(boolean manageHBaseCluster, boolean createTables) {
    HBaseTestsSuite.manageHBaseCluster = manageHBaseCluster;
    HBaseTestsSuite.createTables = createTables;
  }

  public static HBaseAdmin getAdmin() {
    return admin;
  }

}
