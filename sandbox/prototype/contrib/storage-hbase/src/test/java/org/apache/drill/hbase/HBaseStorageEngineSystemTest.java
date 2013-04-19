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
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.exec.ref.*;
import org.apache.drill.exec.ref.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.ref.rse.RSERegistry;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.ScalarValues;
import org.apache.drill.exec.ref.values.SimpleMapValue;
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

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.apache.drill.common.util.FileUtils.getResourceAsFile;

public class HBaseStorageEngineSystemTest {

  private static final byte[] DONUTS_TABLE = Bytes.toBytes("donuts");
  private static final byte[] DONUTS_METADATA_CF = Bytes.toBytes("metadata");
  private static final byte[] DONUTS_BATTERS_CF = Bytes.toBytes("batters");
  private static final byte[] DONUTS_REGION_SEPARATOR = Bytes.toBytes("0003");
  private HTable table;
  private DrillConfig drillConfig;

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
    drillConfig = DrillConfig.create();
    util.startMiniCluster(2);
    try {
      table = util.createTable(DONUTS_TABLE, new byte[][]{DONUTS_METADATA_CF, DONUTS_BATTERS_CF});
      util.createMultiRegions(util.getConfiguration(), table, DONUTS_METADATA_CF,
        new byte[][]{HConstants.EMPTY_BYTE_ARRAY, DONUTS_REGION_SEPARATOR});
    } catch (TableExistsException tee) {
      table = new HTable(util.getConfiguration(), DONUTS_TABLE);
    }
    loadDonuts();
    table.close();
  }

  private void loadDonuts() throws IOException {
    // loads the donuts from JSON
    RecordIterator recordIterator = TestUtils.jsonToRecordIterator("donuts", Files.toString(getResourceAsFile("/simple_donuts.json"), Charsets.UTF_8));
    while (recordIterator.next() != RecordIterator.NextOutcome.NONE_LEFT) {
      RecordPointer pointer = recordIterator.getRecordPointer();
      SimpleMapValue value = (SimpleMapValue) pointer.getField(new SchemaPath("donuts"));


      Iterator<Map.Entry<CharSequence, DataValue>> iterator = value.iterator();
      Map.Entry<CharSequence, DataValue> keyEntry = iterator.next();
      assertTrue(keyEntry.getKey().equals("rowKey"));
      ScalarValues.StringScalar key = (ScalarValues.StringScalar) keyEntry.getValue();
      Put put = new Put(Bytes.toBytes(key.getString().toString()));
      while (iterator.hasNext()) {
        addColumns(iterator.next(), put);
      }
      table.put(put);
    }
  }

  private void addColumns(Map.Entry<CharSequence, DataValue> entry, Put put) {
    for (Map.Entry<CharSequence, DataValue> subEntry : (SimpleMapValue) entry.getValue()) {
      put.add(Bytes.toBytes(entry.getKey().toString()),
        Bytes.toBytes(subEntry.getKey().toString()),
        HbaseUtils.toBytes(subEntry.getValue()));
    }
  }


  @Test
  public void testTableRecordReaderReadEntireTable() throws Exception {
    LogicalPlan plan = LogicalPlan.parse(drillConfig, Files.toString(getResourceAsFile("/simple_hbase_table_plan.json"), Charsets.UTF_8));
    IteratorRegistry ir = new IteratorRegistry();
    RSERegistry rses = new RSERegistry(drillConfig);
    HBaseStorageEngine engine = (HBaseStorageEngine) rses.getEngine(new HBaseStorageEngine.HBaseStorageEngineConfig("hbase"));
    engine.setHBaseConfiguration(util.getConfiguration());
    ReferenceInterpreter i = new ReferenceInterpreter(plan, ir, new BasicEvaluatorFactory(ir), rses);
    i.setup();
    Collection<RunOutcome> outcomes = i.run();
    assertEquals(1, outcomes.size());
    assertEquals(5, outcomes.iterator().next().records);
  }

  @After
  public void stopCluster() throws Exception {
    util.deleteTable(DONUTS_TABLE);
    util.shutdownMiniCluster();
  }

}
