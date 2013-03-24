/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.hbase;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ref.IteratorRegistry;
import org.apache.drill.exec.ref.ReferenceInterpreter;
import org.apache.drill.exec.ref.RunOutcome;
import org.apache.drill.exec.ref.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.ref.rse.RSERegistry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;

import static junit.framework.Assert.assertEquals;

public class HBaseStorageEngineSystemTest {

  private static final byte[] TEST_TABLE = Bytes.toBytes("testtable");
  private static final byte[] TEST_FAMILYA = Bytes.toBytes("testfamilya");
  private static final byte[] TEST_FAMILYB = Bytes.toBytes("testfamilyb");
  private static final byte[] TEST_QUALIFIER = Bytes.toBytes("testcolumn");
  private static final byte[] TEST_MULTI_CQ = Bytes.toBytes("TestMultiCQ");

  private static byte[] ROW = Bytes.toBytes("testRow");
  private static final int ROWSIZE = 20;
  private static final int rowSeperator1 = 5;
  private static final int rowSeperator2 = 12;
  private static byte[][] ROWS = makeN(ROW, ROWSIZE);
  private HTable table;

  private static byte[][] makeN(byte[] base, int n) {
    byte[][] ret = new byte[n][];
    for (int i = 0; i < n; i++) {
      ret[i] = Bytes.add(base, Bytes.toBytes(i));
    }
    return ret;
  }

  private static HBaseTestingUtility util;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      ConstantSizeRegionSplitPolicy.class.getName());
    util = new HBaseTestingUtility(conf);
  }

  @Before
  public void startCluster() throws Exception {
    util.startMiniCluster(2);
    try {
      table = util.createTable(TEST_TABLE, new byte[][]{TEST_FAMILYA});
      util.createMultiRegions(util.getConfiguration(), table, TEST_FAMILYA,
        new byte[][]{HConstants.EMPTY_BYTE_ARRAY, ROWS[rowSeperator1],
          ROWS[rowSeperator2]});
    } catch (TableExistsException tee) {
      table = new HTable(util.getConfiguration(), TEST_TABLE);
    }

    for (int i = 0; i < ROWSIZE; i++) {
      Put put = new Put(ROWS[i]);
      Long l = (long) i;
      put.add(TEST_FAMILYA, TEST_QUALIFIER, Bytes.toBytes(l));
      table.put(put);
      Put p2 = new Put(ROWS[i]);
      p2.add(TEST_FAMILYA, Bytes.add(TEST_MULTI_CQ, Bytes.toBytes(l)), Bytes
        .toBytes(l * 10));
      table.put(p2);
    }
    table.close();
  }

  @Test
  public void testTableStorageEngine() throws Exception {
    DrillConfig config = DrillConfig.create();
    LogicalPlan plan = LogicalPlan.parse(config, Files.toString(FileUtils.getResourceAsFile("/simple_hbase_plan.json"), Charsets.UTF_8));
    IteratorRegistry ir = new IteratorRegistry();
    RSERegistry rses = new RSERegistry(config);
    HBaseStorageEngine engine = (HBaseStorageEngine) rses.getEngine(new HBaseStorageEngine.HBaseStorageEngineConfig("hbase"));
    engine.setHBaseConfiguration(util.getConfiguration());
    ReferenceInterpreter i = new ReferenceInterpreter(plan, ir, new BasicEvaluatorFactory(ir), rses);
    i.setup();
    Collection<RunOutcome> outcomes = i.run();
    assertEquals(1, outcomes.size());
    assertEquals(19, outcomes.iterator().next().records);
  }

  @After
  public void stopCluster() throws Exception {
    util.deleteTable(TEST_TABLE);
    util.shutdownMiniCluster();
  }

}
