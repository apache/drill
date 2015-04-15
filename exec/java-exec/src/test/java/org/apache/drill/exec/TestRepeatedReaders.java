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

package org.apache.drill.exec;

import org.apache.drill.BaseTestQuery;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRepeatedReaders extends BaseTestQuery {

  static FileSystem fs;

  @BeforeClass
  public static void initFs() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "local");

    fs = FileSystem.get(conf);
  }

  private static void deleteTableIfExists(String tableName) {
    try {
      Path path = new Path(getDfsTestTmpSchemaLocation(), tableName);
      if (fs.exists(path)) {
        fs.delete(path, true);
      }
    } catch (Exception e) {
      // ignore exceptions.
    }
  }

  private void createAndQuery(String datafile) throws Exception {
    String query = String.format("select * from cp.`parquet/%s`", datafile);
    String tableName = "test_repeated_readers_"+datafile;

    try {
      test("create table dfs_test.tmp.`%s` as %s", tableName, query);

      testBuilder()
        .sqlQuery("select * from dfs_test.tmp.`%s` d", tableName)
        .ordered()
        .jsonBaselineFile("parquet/" + datafile)
        .go();
    } finally {
      deleteTableIfExists(tableName);
    }
  }

  @Test //DRILL-2292
  public void testNestedRepeatedMapInsideRepeatedMap() throws Exception {
    createAndQuery("2292.rm_rm.json");
  }

  @Test //DRILL-2292
  public void testNestedRepeatedMapInsideMapInsideRepeatedMap() throws Exception {
    createAndQuery("2292.rm_m_rm.json");
  }

  @Test //DRILL-2292
  public void testNestedRepeatedListInsideRepeatedMap() throws Exception {
    runSQL("alter session set `store.format` = 'json'");

    try {
      createAndQuery("2292.rl_rm.json");
    } finally {
      runSQL("alter session set `store.format` = 'parquet'");
    }
  }

  @Test //DRILL-2292
  public void testNestedRepeatedMapInsideRepeatedList() throws Exception {
    runSQL("alter session set `store.format` = 'json'");

    try {
      createAndQuery("2292.rm_rl.json");
    } finally {
      runSQL("alter session set `store.format` = 'parquet'");
    }
  }

  @Test //DRILL-2292
  public void testNestedRepeatedListInsideRepeatedList() throws Exception {
    runSQL("alter session set `store.format` = 'json'");

    try {
      createAndQuery("2292.rl_rl.json");
    } finally {
      runSQL("alter session set `store.format` = 'parquet'");
    }
  }
}
