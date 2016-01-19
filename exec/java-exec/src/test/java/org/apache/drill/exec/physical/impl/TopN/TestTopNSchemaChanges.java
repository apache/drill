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
package org.apache.drill.exec.physical.impl.TopN;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.TestBuilder;
import org.apache.drill.exec.physical.impl.aggregate.InternalBatch;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class TestTopNSchemaChanges extends BaseTestQuery {

  @Test
  public void testNumericTypes() throws Exception {
    final File data_dir = new File(BaseTestQuery.getTempDir("topn-schemachanges"));
    data_dir.mkdirs();

    // left side int and strings
    BufferedWriter writer = new BufferedWriter(new FileWriter(new File(data_dir, "d1.json")));
    for (int i = 0; i < 10000; i+=2) {
      writer.write(String.format("{ \"kl\" : %d , \"vl\": %d }\n", i, i));
    }
    writer.close();
    writer = new BufferedWriter(new FileWriter(new File(data_dir, "d2.json")));
    for (int i = 1; i < 10000; i+=2) {
      writer.write(String.format("{ \"kl\" : %f , \"vl\": %f }\n", (float)i, (float)i));
    }
    writer.close();
    String query = String.format("select * from dfs_test.`%s` order by kl limit 12", data_dir.toPath().toString());

    TestBuilder builder = testBuilder()
      .sqlQuery(query)
      .optionSettingQueriesForTestQuery("alter session set `exec.enable_union_type` = true")
      .ordered()
      .baselineColumns("kl", "vl");

    for (long i = 0; i< 12 ; ++i) {
      if (i %2 == 0) {
        builder.baselineValues(i, i);
      } else {
        builder.baselineValues((double)i, (double)i);
      }
    }
    builder.go();
  }

  @Test
  public void testNumericAndStringTypes() throws Exception {
    final File data_dir = new File(BaseTestQuery.getTempDir("topn-schemachanges"));
    data_dir.mkdirs();

    // left side int and strings
    BufferedWriter writer = new BufferedWriter(new FileWriter(new File(data_dir, "d1.json")));
    for (int i = 0; i < 1000; i+=2) {
      writer.write(String.format("{ \"kl\" : %d , \"vl\": %d }\n", i, i));
    }
    writer.close();
    writer = new BufferedWriter(new FileWriter(new File(data_dir, "d2.json")));
    for (int i = 1; i < 1000; i+=2) {
      writer.write(String.format("{ \"kl\" : \"%s\" , \"vl\": \"%s\" }\n", i, i));
    }
    writer.close();
    String query = String.format("select * from dfs_test.`%s` order by kl limit 12", data_dir.toPath().toString());

    TestBuilder builder = testBuilder()
      .sqlQuery(query)
      .optionSettingQueriesForTestQuery("alter session set `exec.enable_union_type` = true")
      .ordered()
      .baselineColumns("kl", "vl");

    for (long i = 0; i< 24 ; i+=2) {
        builder.baselineValues(i, i);
    }

    query = String.format("select * from dfs_test.`%s` order by kl desc limit 12", data_dir.toPath().toString());
    builder = testBuilder()
      .sqlQuery(query)
      .optionSettingQueriesForTestQuery("alter session set `exec.enable_union_type` = true")
      .ordered()
      .baselineColumns("kl", "vl")
      .baselineValues("999", "999")
      .baselineValues("997", "997")
      .baselineValues("995", "995")
      .baselineValues("993", "993")
      .baselineValues("991", "991")
      .baselineValues("99", "99")
      .baselineValues("989", "989")
      .baselineValues("987", "987")
      .baselineValues("985", "985")
      .baselineValues("983", "983")
      .baselineValues("981", "981")
      .baselineValues("979", "979");
    builder.go();
  }

  @Test
  public void testUnionTypes() throws Exception {
    final File data_dir = new File(BaseTestQuery.getTempDir("topn-schemachanges"));
    data_dir.mkdirs();

    // union of int and float and string.
    BufferedWriter writer = new BufferedWriter(new FileWriter(new File(data_dir, "d1.json")));
    for (int i = 0; i <= 9; ++i) {
      switch (i%3) {
        case 0: // 0, 3, 6, 9
          writer.write(String.format("{ \"kl\" : %d , \"vl\": %d }\n", i, i));
          break;
        case 1: // 1, 4, 7
          writer.write(String.format("{ \"kl\" : %f , \"vl\": %f }\n", (float)i, (float)i));
          break;
        case 2: // 2, 5, 8
          writer.write(String.format("{ \"kl\" : \"%s\" , \"vl\": \"%s\" }\n", i, i));
          break;
      }
    }
    writer.close();
    String query = String.format("select * from dfs_test.`%s` order by kl limit 8", data_dir.toPath().toString());

    TestBuilder builder = testBuilder()
      .sqlQuery(query)
      .optionSettingQueriesForTestQuery("alter session set `exec.enable_union_type` = true")
      .ordered()
      .baselineColumns("kl", "vl");

    builder.baselineValues(0l, 0l);
    builder.baselineValues(1.0d, 1.0d);
    builder.baselineValues(3l, 3l);
    builder.baselineValues(4.0d, 4.0d);
    builder.baselineValues(6l, 6l);
    builder.baselineValues(7.0d, 7.0d);
    builder.baselineValues(9l, 9l);
    builder.baselineValues("2", "2");
    builder.go();
  }

  @Test
  public void testMissingColumn() throws Exception {
    final File data_dir = new File(BaseTestQuery.getTempDir("topn-schemachanges"));
    data_dir.mkdirs();
    System.out.println(data_dir);
    BufferedWriter writer = new BufferedWriter(new FileWriter(new File(data_dir, "d1.json")));
    for (int i = 0; i < 100; i++) {
      writer.write(String.format("{ \"kl1\" : %d , \"vl1\": %d }\n", i, i));
    }
    writer.close();
    writer = new BufferedWriter(new FileWriter(new File(data_dir, "d2.json")));
    for (int i = 100; i < 200; i++) {
      writer.write(String.format("{ \"kl\" : %f , \"vl\": %f }\n", (float)i, (float)i));
    }
    writer.close();
    writer = new BufferedWriter(new FileWriter(new File(data_dir, "d3.json")));
    for (int i = 200; i < 300; i++) {
      writer.write(String.format("{ \"kl2\" : \"%s\" , \"vl2\": \"%s\" }\n", i, i));
    }
    writer.close();

    String query = String.format("select kl, vl, kl1, vl1, kl2, vl2 from dfs_test.`%s` order by kl limit 3", data_dir.toPath().toString());
    TestBuilder builder = testBuilder()
      .sqlQuery(query)
      .optionSettingQueriesForTestQuery("alter session set `exec.enable_union_type` = true")
      .ordered()
      .baselineColumns("kl", "vl", "kl1", "vl1", "kl2", "vl2")
      .baselineValues(100.0d, 100.0d, null, null, null, null)
      .baselineValues(101.0d, 101.0d, null, null, null, null)
      .baselineValues(102.0d, 102.0d, null, null, null, null);
    builder.go();

    query = String.format("select kl, vl, kl1, vl1, kl2, vl2  from dfs_test.`%s` order by kl1 limit 3", data_dir.toPath().toString());
    builder = testBuilder()
      .sqlQuery(query)
      .optionSettingQueriesForTestQuery("alter session set `exec.enable_union_type` = true")
      .ordered()
      .baselineColumns("kl", "vl", "kl1", "vl1", "kl2", "vl2")
      .baselineValues(null, null, 0l, 0l, null, null)
      .baselineValues(null, null, 1l, 1l, null, null)
      .baselineValues(null, null, 2l, 2l, null, null);
    builder.go();

    query = String.format("select kl, vl, kl1, vl1, kl2, vl2 from dfs_test.`%s` order by kl2 desc limit 3", data_dir.toPath().toString());
    builder = testBuilder()
      .sqlQuery(query)
      .optionSettingQueriesForTestQuery("alter session set `exec.enable_union_type` = true")
      .ordered()
      .baselineColumns("kl", "vl", "kl1", "vl1", "kl2", "vl2")
      .baselineValues(null, null, null, null, "299", "299")
      .baselineValues(null, null, null, null, "298", "298")
      .baselineValues(null, null, null, null, "297", "297");
    builder.go();
    // Since client can't handle new columns which are not in first batch, we won't test output of query.
    // Query should run w/o any errors.
    test(String.format("select * from dfs_test.`%s` order by kl limit 3", data_dir.toPath().toString()));
  }
}
