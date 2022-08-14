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
package org.apache.drill.exec.store.phoenix;

import static org.apache.hadoop.hbase.HConstants.HBASE_DIR;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_CAT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.phoenix.util.PhoenixRuntime;
import org.slf4j.LoggerFactory;

/**
 * This is a copy of {@code org.apache.phoenix.end2end.QueryServerBasicsIT} until
 * <a href="https://issues.apache.org/jira/browse/PHOENIX-6613">PHOENIX-6613</a> is fixed
 */
public class PhoenixBasicsIT {
  private static final HBaseTestingUtility util = new HBaseTestingUtility();

  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(PhoenixBasicsIT.class);

  protected static String CONN_STRING;
  static LocalHBaseCluster hbaseCluster;

  public static synchronized void doSetup() throws Exception {
    Configuration conf = util.getConfiguration();
    // Start ZK by hand
    util.startMiniZKCluster();
    Path rootdir = util.getDataTestDirOnTestFS(PhoenixBasicsIT.class.getSimpleName());
    // There is no setRootdir method that is available in all supported HBase versions.
    conf.set(HBASE_DIR, rootdir.toString());
    hbaseCluster = new LocalHBaseCluster(conf, 1);
    hbaseCluster.startup();

    CONN_STRING = PhoenixRuntime.JDBC_PROTOCOL + ":localhost:" + getZookeeperPort();
    logger.info("JDBC connection string is " + CONN_STRING);
  }

  public static int getZookeeperPort() {
    return util.getConfiguration().getInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181);
  }

  public static void testCatalogs() throws Exception {
    try (final Connection connection = DriverManager.getConnection(CONN_STRING)) {
      assertFalse(connection.isClosed());
      try (final ResultSet resultSet = connection.getMetaData().getCatalogs()) {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        assertFalse("unexpected populated resultSet", resultSet.next());
        assertEquals(1, metaData.getColumnCount());
        assertEquals(TABLE_CAT, metaData.getColumnName(1));
      }
    }
  }

  public static synchronized void afterClass() throws IOException {
    Optional.of(hbaseCluster).ifPresent(LocalHBaseCluster::shutdown);
    util.shutdownMiniCluster();
  }
}
