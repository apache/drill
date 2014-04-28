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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.drill.common.util.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.mapred.TestTableInputFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

@RunWith(Suite.class)
@SuiteClasses({HBaseRecordReaderTest.class})
public class HBaseTestsSuite {
  private static final Log LOG = LogFactory.getLog(TestTableInputFormat.class);
  private static final boolean IS_DEBUG = ManagementFactory.getRuntimeMXBean().getInputArguments().toString().indexOf("-agentlib:jdwp") > 0;

  private static Configuration conf;

  private static HBaseTestingUtility UTIL;

  @BeforeClass
  public static void setUp() throws Exception {
    if (conf == null) {
      conf = HBaseConfiguration.create();
    }
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    if (IS_DEBUG) {
      conf.set("hbase.regionserver.lease.period","1000000");
    }
    LOG.info("Starting HBase mini cluster.");
    if (UTIL == null) {
      UTIL = new HBaseTestingUtility(conf);
    }
    UTIL.startMiniCluster();
    LOG.info("HBase mini cluster started.");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    LOG.info("Shutting down HBase mini cluster.");
    UTIL.shutdownMiniCluster();
    LOG.info("HBase mini cluster stopped.");
  }

  public static Configuration getConf() {
    return conf;
  }

  public static String getPlanText(String planFile) throws IOException {
    String text = Files.toString(FileUtils.getResourceAsFile(planFile), Charsets.UTF_8);
    return text.replaceFirst("\"zookeeperPort\".*:.*\\d+", "\"zookeeperPort\" : " 
        + conf.get(HConstants.ZOOKEEPER_CLIENT_PORT));
  }

  public static HBaseTestingUtility getHBaseTestingUtility() {
    return UTIL;
  }
}
