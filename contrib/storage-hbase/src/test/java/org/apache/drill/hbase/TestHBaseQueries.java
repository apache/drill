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

import java.util.Arrays;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Test;

public class TestHBaseQueries extends BaseHBaseTest {

  @Test
  public void testWithEmptyFirstAndLastRegion() throws Exception {
    HBaseAdmin admin = HBaseTestsSuite.getAdmin();
    String tableName = "drill_ut_empty_regions";
    HTable table = null;

    try {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("f"));
    admin.createTable(desc, Arrays.copyOfRange(TestTableGenerator.SPLIT_KEYS, 0, 2));

    table = new HTable(admin.getConfiguration(), tableName);
    Put p = new Put("b".getBytes());
    p.add("f".getBytes(), "c".getBytes(), "1".getBytes());
    table.put(p);

    setColumnWidths(new int[] {8, 15});
    runHBaseSQLVerifyCount("SELECT *\n"
        + "FROM\n"
        + "  hbase.`" + tableName + "` tableName\n"
        , 1);
    } finally {
      try {
        if (table != null) {
          table.close();
        }
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      } catch (Exception e) { } // ignore
    }

  }
}
