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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class TestTableGenerator {

  static final byte[][] SPLIT_KEYS = {
    {'b'}, {'c'}, {'d'}, {'e'}, {'f'}, {'g'}, {'h'}, {'i'},
    {'j'}, {'k'}, {'l'}, {'m'}, {'n'}, {'o'}, {'p'}, {'q'},
    {'r'}, {'s'}, {'t'}, {'u'}, {'v'}, {'w'}, {'x'}, {'y'}, {'z'}
  };
  
  public static void generateHBaseTable(HBaseAdmin admin, String tableName, int numberRegions,
      int recordsPerRegion) throws Exception {
    Configuration conf = admin.getConfiguration();
    
    byte[][] splitKeys = Arrays.copyOfRange(SPLIT_KEYS, 0, numberRegions-1);
    
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("f"));
    desc.addFamily(new HColumnDescriptor("f2"));
    admin.createTable(desc, splitKeys);
    HTable table = new HTable(HBaseTestsSuite.getConf(), tableName);
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
  }

}
