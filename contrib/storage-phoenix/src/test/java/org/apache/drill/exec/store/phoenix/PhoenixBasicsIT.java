/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.drill.exec.store.phoenix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.util.PhoenixRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import static org.apache.hadoop.hbase.HConstants.HBASE_DIR;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PhoenixBasicsIT {
  private static final HBaseTestingUtility util = new HBaseTestingUtility();
  private static final Logger logger = LoggerFactory.getLogger(PhoenixBasicsIT.class);

  protected static String CONN_STRING;

  public static synchronized void doSetup() throws Exception {
    Configuration conf = util.getConfiguration();

    // Keep it embedded & filesystem-only (no HDFS)
    conf.set("hbase.cluster.distributed", "false");
    conf.setBoolean("hbase.unsafe.stream.capability.enforce", false);
    conf.setInt("hbase.master.wait.on.regionservers.mintostart", 1);

    // Randomize service ports, disable HTTP/Jetty info servers to avoid Netty/servlet deps
    conf.setInt("hbase.master.port", 0);
    conf.setInt("hbase.master.info.port", -1);
    conf.setInt("hbase.regionserver.port", 0);
    conf.setInt("hbase.regionserver.info.port", -1);
    conf.unset("hbase.http.filter.initializers"); // make sure no web filters get bootstrapped

    // Force loopback to dodge IPv6/hostname hiccups
    conf.set("hbase.zookeeper.quorum", "127.0.0.1");
    conf.set("hbase.master.hostname", "127.0.0.1");
    conf.set("hbase.regionserver.hostname", "127.0.0.1");

    // Root dir on local FS (file:///), so HTU won't start MiniDFS
    Path rootdir = util.getDataTestDirOnTestFS(PhoenixBasicsIT.class.getSimpleName());
    conf.set(HBASE_DIR, rootdir.toUri().toString()); // keep URI form

    // Start ZK + 1 Master + 1 RegionServer WITHOUT HDFS
    util.startMiniZKCluster();
    util.startMiniHBaseCluster(1, 1);

    int zkPort = util.getZkCluster().getClientPort();
    CONN_STRING = PhoenixRuntime.JDBC_PROTOCOL + ":localhost:" + zkPort;
    logger.info("JDBC connection string is {}", CONN_STRING);
  }

  public static void testCatalogs() throws Exception {
    try (Connection connection = DriverManager.getConnection(CONN_STRING)) {
      assertFalse(connection.isClosed());
      try (ResultSet rs = connection.getMetaData().getCatalogs()) {
        ResultSetMetaData md = rs.getMetaData();
        String col = md.getColumnLabel(1);  // label is safer than name
        if (!"TABLE_CAT".equals(col) && !"TENANT_ID".equals(col)) {
          // fall back to name just in case some drivers differ
          col = md.getColumnName(1);
        }
        assertTrue("Unexpected first column: " + col,
            "TABLE_CAT".equals(col) || "TENANT_ID".equals(col));
      }
    }
  }

  public static synchronized void afterClass() throws IOException {
    util.shutdownMiniHBaseCluster();  // stops RS & Master
    util.shutdownMiniZKCluster();     // stops ZK
  }
}
