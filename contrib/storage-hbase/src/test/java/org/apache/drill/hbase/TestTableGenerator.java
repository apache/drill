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
import java.util.Random;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class TestTableGenerator {

  public static final byte[][] SPLIT_KEYS = {
    {'b'}, {'c'}, {'d'}, {'e'}, {'f'}, {'g'}, {'h'}, {'i'},
    {'j'}, {'k'}, {'l'}, {'m'}, {'n'}, {'o'}, {'p'}, {'q'},
    {'r'}, {'s'}, {'t'}, {'u'}, {'v'}, {'w'}, {'x'}, {'y'}, {'z'}
  };

  static final byte[] FAMILY_F = {'f'};
  static final byte[] COLUMN_C = {'c'};

  public static void generateHBaseDataset1(HBaseAdmin admin, String tableName, int numberRegions) throws Exception {
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }

    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("f"));
    desc.addFamily(new HColumnDescriptor("f2"));
    if (numberRegions > 1) {
      admin.createTable(desc, Arrays.copyOfRange(SPLIT_KEYS, 0, numberRegions-1));
    } else {
      admin.createTable(desc);
    }

    HTable table = new HTable(admin.getConfiguration(), tableName);

    Put p = new Put("a1".getBytes());
    p.add("f".getBytes(), "c1".getBytes(), "1".getBytes());
    p.add("f".getBytes(), "c2".getBytes(), "2".getBytes());
    p.add("f".getBytes(), "c3".getBytes(), "3".getBytes());
    p.add("f".getBytes(), "c4".getBytes(), "4".getBytes());
    p.add("f".getBytes(), "c5".getBytes(), "5".getBytes());
    p.add("f".getBytes(), "c6".getBytes(), "6".getBytes());
    table.put(p);

    p = new Put("a2".getBytes());
    p.add("f".getBytes(), "c1".getBytes(), "1".getBytes());
    p.add("f".getBytes(), "c2".getBytes(), "2".getBytes());
    p.add("f".getBytes(), "c3".getBytes(), "3".getBytes());
    p.add("f".getBytes(), "c4".getBytes(), "4".getBytes());
    p.add("f".getBytes(), "c5".getBytes(), "5".getBytes());
    p.add("f".getBytes(), "c6".getBytes(), "6".getBytes());
    table.put(p);

    p = new Put("a3".getBytes());
    p.add("f".getBytes(), "c1".getBytes(), "1".getBytes());
    p.add("f".getBytes(), "c3".getBytes(), "2".getBytes());
    p.add("f".getBytes(), "c5".getBytes(), "3".getBytes());
    p.add("f".getBytes(), "c7".getBytes(), "4".getBytes());
    p.add("f".getBytes(), "c8".getBytes(), "5".getBytes());
    p.add("f".getBytes(), "c9".getBytes(), "6".getBytes());
    table.put(p);

    p = new Put("b4".getBytes());
    p.add("f".getBytes(), "c1".getBytes(), "1".getBytes());
    p.add("f2".getBytes(), "c2".getBytes(), "2".getBytes());
    p.add("f".getBytes(), "c3".getBytes(), "3".getBytes());
    p.add("f2".getBytes(), "c4".getBytes(), "4".getBytes());
    p.add("f".getBytes(), "c5".getBytes(), "5".getBytes());
    p.add("f2".getBytes(), "c6".getBytes(), "6".getBytes());
    table.put(p);

    p = new Put("b5".getBytes());
    p.add("f2".getBytes(), "c1".getBytes(), "1".getBytes());
    p.add("f".getBytes(), "c2".getBytes(), "2".getBytes());
    p.add("f2".getBytes(), "c3".getBytes(), "3".getBytes());
    p.add("f".getBytes(), "c4".getBytes(), "4".getBytes());
    p.add("f2".getBytes(), "c5".getBytes(), "5".getBytes());
    p.add("f".getBytes(), "c6".getBytes(), "6".getBytes());
    table.put(p);

    p = new Put("b6".getBytes());
    p.add("f".getBytes(), "c1".getBytes(), "1".getBytes());
    p.add("f2".getBytes(), "c3".getBytes(), "2".getBytes());
    p.add("f".getBytes(), "c5".getBytes(), "3".getBytes());
    p.add("f2".getBytes(), "c7".getBytes(), "4".getBytes());
    p.add("f".getBytes(), "c8".getBytes(), "5".getBytes());
    p.add("f2".getBytes(), "c9".getBytes(), "6".getBytes());
    table.put(p);
    table.flushCommits();
    table.close();
  }

  public static void generateHBaseDataset2(HBaseAdmin admin, String tableName, int numberRegions) throws Exception {
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }

    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("f"));

    if (numberRegions > 1) {
      admin.createTable(desc, Arrays.copyOfRange(SPLIT_KEYS, 0, numberRegions-1));
    } else {
      admin.createTable(desc);
    }

    HTable table = new HTable(admin.getConfiguration(), tableName);

    int rowCount = 0;
    byte[] bytes = null;
    final int numColumns = 5;
    Random random = new Random();
    int iteration = 0;
    while (rowCount < 1000) {
      char rowKeyChar = 'a';
      for (int i = 0; i < numberRegions; i++) {
        Put p = new Put((""+rowKeyChar+iteration).getBytes());
        for (int j = 1; j <= numColumns; j++) {
          bytes = new byte[5000]; random.nextBytes(bytes);
          p.add("f".getBytes(), ("c"+j).getBytes(), bytes);
        }
        table.put(p);

        ++rowKeyChar;
        ++rowCount;
      }
      ++iteration;
    }

    table.flushCommits();
    table.close();

    admin.flush(tableName);
  }

  public static void generateHBaseDataset3(HBaseAdmin admin, String tableName, int numberRegions) throws Exception {
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }

    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(FAMILY_F));

    if (numberRegions > 1) {
      admin.createTable(desc, Arrays.copyOfRange(SPLIT_KEYS, 0, numberRegions-1));
    } else {
      admin.createTable(desc);
    }

    HTable table = new HTable(admin.getConfiguration(), tableName);

    for (int i = 0; i <= 100; ++i) {
      Put p = new Put((String.format("%03d", i)).getBytes());
      p.add(FAMILY_F, COLUMN_C, String.format("value %03d", i).getBytes());
      table.put(p);
    }
    for (int i = 0; i <= 1000; ++i) {
      Put p = new Put((String.format("%04d", i)).getBytes());
      p.add(FAMILY_F, COLUMN_C, String.format("value %04d", i).getBytes());
      table.put(p);
    }

    Put p = new Put("%_AS_PREFIX_ROW1".getBytes());
    p.add(FAMILY_F, COLUMN_C, "dummy".getBytes());
    table.put(p);

    p = new Put("%_AS_PREFIX_ROW2".getBytes());
    p.add(FAMILY_F, COLUMN_C, "dummy".getBytes());
    table.put(p);

    table.flushCommits();
    table.close();

    admin.flush(tableName);
  }

}
