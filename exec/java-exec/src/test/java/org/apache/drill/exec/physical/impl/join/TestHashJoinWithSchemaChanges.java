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
package org.apache.drill.exec.physical.impl.join;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.TestBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class TestHashJoinWithSchemaChanges extends BaseTestQuery {

  @Test
  public void testHashInnerJoinWithSchemaChange() throws Exception {
    String query = "select A.a as aa, A.b as ab, B.a as ba, B.b as bb from cp.`join/schemachange/left.json` A inner join cp.`join/schemachange/right.json` B on A.a=B.a";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .optionSettingQueriesForTestQuery("alter session set `planner.enable_mergejoin` = false; alter session set `exec.enable_union_type` = true")
      .baselineColumns("aa", "ab", "ba", "bb")
      .baselineValues(1l, 1l, 1l, 1l)
      .baselineValues(1l, 1l, 1.0d, "1")
      .baselineValues(1l, 1l, 1l, 1l)
      .baselineValues(1l, 1l, 1.0d, "1")
      .baselineValues(2l, 2l, 2l, 2l)
      .baselineValues(2l, 2l, 2.0d, "2")
      .baselineValues(2l, 2l, 2l, 2l)
      .baselineValues(2l, 2l, 2.0d, "2").build().run();
  }

  @Test
  public void testHashLeftJoinWithSchemaChange() throws Exception {
    String query = "select A.a as aa, A.b as ab, B.a as ba, B.b as bb from cp.`join/schemachange/left.json` A left join cp.`join/schemachange/right.json` B on A.a=B.a";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .optionSettingQueriesForTestQuery("alter session set `planner.enable_mergejoin` = false; alter session set `exec.enable_union_type` = true")
      .baselineColumns("aa", "ab", "ba", "bb")
      .baselineValues(1l, 1l, 1l, 1l)
      .baselineValues(1l, 1l, 1.0d, "1")
      .baselineValues(1l, 1l, 1l, 1l)
      .baselineValues(1l, 1l, 1.0d, "1")
      .baselineValues(2l, 2l, 2l, 2l)
      .baselineValues(2l, 2l, 2.0d, "2")
      .baselineValues(2l, 2l, 2l, 2l)
      .baselineValues(2l, 2l, 2.0d, "2")
      .baselineValues(3l, 3l, null, null)
      .baselineValues(3l, 3l, null, null)
      .build().run();
  }

  @Test
  public void testHashRightJoinWithSchemaChange() throws Exception {
    String query = "select A.a as aa, A.b as ab, B.a as ba, B.b as bb from cp.`join/schemachange/left.json` A right join cp.`join/schemachange/right.json` B on A.a=B.a";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .optionSettingQueriesForTestQuery("alter session set `planner.enable_mergejoin` = false; alter session set `exec.enable_union_type` = true")
      .baselineColumns("aa", "ab", "ba", "bb")
      .baselineValues(1l, 1l, 1l, 1l)
      .baselineValues(1l, 1l, 1.0d, "1")
      .baselineValues(1l, 1l, 1l, 1l)
      .baselineValues(1l, 1l, 1.0d, "1")
      .baselineValues(2l, 2l, 2l, 2l)
      .baselineValues(2l, 2l, 2.0d, "2")
      .baselineValues(2l, 2l, 2l, 2l)
      .baselineValues(2l, 2l, 2.0d, "2")
      .baselineValues(null, null, 6.0d, 6l)
      .build().run();
  }

  @Test
  public void testHashJoinWithSchemaChangeMultiBatch() throws Exception {
    final File leftSideDir = new File(BaseTestQuery.getTempDir("hashjoin-left"));
    final File rightSideDir = new File(BaseTestQuery.getTempDir("hashjoin-right"));

    leftSideDir.mkdirs();
    rightSideDir.mkdirs();

    System.out.println("left = " + leftSideDir);
    System.out.println("right = " + rightSideDir);

    // LEFT side
    // int, int
    BufferedWriter leftWriter = new BufferedWriter(new FileWriter(new File(leftSideDir, "1.json")));
    for (int i = 1; i <= 10; ++i) {
      leftWriter.write(String.format("{ \"lk\" : %d , \"lv\": %d }\n", i, i));
    }
    leftWriter.close();

    // float float , new column
    leftWriter = new BufferedWriter(new FileWriter(new File(leftSideDir, "2.json")));
    for (int i = 5; i <= 15; ++i) {
      leftWriter.write(String.format("{ \"lk\" : %f , \"lv\": %f , \"lc\": %d}\n", (float) i, (float) i, i));
    }
    leftWriter.close();

    // string, string
    leftWriter = new BufferedWriter(new FileWriter(new File(leftSideDir, "3.json")));
    for (int i = 1; i <= 10; ++i) {
      leftWriter.write(String.format("{ \"lk\" : \"%s\" , \"lv\": \"%s\"}\n", i, i));
    }
    leftWriter.close();

    // RIGHT side
    BufferedWriter rightWriter = new BufferedWriter(new FileWriter(new File(rightSideDir, "1.json")));
    for (int i = 5; i <= 10; ++i) {
      rightWriter.write(String.format("{ \"rk\" : %d , \"rv\": %d }\n", i, i));
    }
    rightWriter.close();
    // float float new column
    rightWriter = new BufferedWriter(new FileWriter(new File(rightSideDir, "2.json")));
    for (int i = 5; i <= 10; ++i) {
      rightWriter.write(String.format("{ \"rk\" : %f , \"rv\": %f , \"rc\": %d }\n", (float) i, (float) i, i));
    }
    rightWriter.close();
    // string string
    rightWriter = new BufferedWriter(new FileWriter(new File(rightSideDir, "3.json")));
    for (int i = 5; i <= 15; ++i) {
      rightWriter.write(String.format("{ \"rk\" : \"%s\", \"rv\": \"%s\" }\n", Integer.toString(i), Integer.toString(i)));
    }
    rightWriter.close();

    // Test inner join. since we don't have full schema select all columns for now.
    final String innerJoinQuery = String.format("select L.lk, L.lv, L.lc, R.rk, R.rv, R.rc from dfs_test.`%s` L inner join dfs_test.`%s` R on L.lk=R.rk",
      leftSideDir.toPath().toString(), rightSideDir.toPath().toString());
    final String leftJoinQuery = String.format("select L.lk, L.lv, L.lc, R.rk, R.rv, R.rc from dfs_test.`%s` L left join dfs_test.`%s` R on L.lk=R.rk",
      leftSideDir.toPath().toString(), rightSideDir.toPath().toString());
    final String rightJoinQuery = String.format("select L.lk, L.lv, L.lc, R.rk, R.rv, R.rc from dfs_test.`%s` L right join dfs_test.`%s` R on L.lk=R.rk",
      leftSideDir.toPath().toString(), rightSideDir.toPath().toString());
    final String outerJoinQuery = String.format("select L.lk, L.lv, L.lc, R.rk, R.rv, R.rc from dfs_test.`%s` L full outer join dfs_test.`%s` R on L.lk=R.rk",
      leftSideDir.toPath().toString(), rightSideDir.toPath().toString());

    TestBuilder builder = testBuilder()
      .sqlQuery(innerJoinQuery)
      .unOrdered()
      .optionSettingQueriesForTestQuery("alter session set `planner.enable_mergejoin` = false; alter session set `exec.enable_union_type` = true")
      .baselineColumns("lk", "lv", "lc", "rk", "rv", "rc");
    for (long i = 5; i <= 10; i++) {
      final String stringVal = Long.toString(i);
      // match int with int and float
      builder.baselineValues(i, i, null, i, i, null);
      builder.baselineValues(i, i, null, (double)i, (double)i, i);
      // match float with int and float
      builder.baselineValues((double)i, (double)i, i, i, i, null);
      builder.baselineValues((double)i, (double)i, i, (double)i, (double)i, i);
      // match string with string
      builder.baselineValues(stringVal, stringVal, null, stringVal, stringVal, null);
    }
    builder.build().run();

    // LEFT
    builder = testBuilder()
      .sqlQuery(leftJoinQuery)
      .unOrdered()
      .optionSettingQueriesForTestQuery("alter session set `planner.enable_mergejoin` = false; alter session set `exec.enable_union_type` = true")
      .baselineColumns("lk", "lv", "lc", "rk", "rv", "rc");
    for (long i = 5; i <= 10; i++) {
      final String stringVal = Long.toString(i);
      // match int with int and float
      builder.baselineValues(i, i, null, i, i, null);
      builder.baselineValues(i, i, null, (double)i, (double)i, i);
      // match float with int and float
      builder.baselineValues((double)i, (double)i, i, i, i, null);
      builder.baselineValues((double)i, (double)i, i, (double)i, (double)i, i);
      // match string with string
      builder.baselineValues(stringVal, stringVal, null, stringVal, stringVal, null);
    }
    for (long i = 1; i < 5; i++) {
      final String stringVal = Long.toString(i);
      builder.baselineValues(i, i, null, null, null, null);
      builder.baselineValues(stringVal, stringVal, null, null, null, null);
    }
    for (long i = 11; i <= 15; i++) {
      builder.baselineValues((double)i, (double)i, i, null, null, null);
    }
    builder.build().run();

    // RIGHT
    builder = testBuilder()
      .sqlQuery(rightJoinQuery)
      .unOrdered()
      .optionSettingQueriesForTestQuery("alter session set `planner.enable_mergejoin` = false; alter session set `exec.enable_union_type` = true")
      .baselineColumns("lk", "lv", "lc", "rk", "rv", "rc");
    for (long i = 5; i <= 10; i++) {
      final String stringVal = Long.toString(i);
      // match int with int and float
      builder.baselineValues(i, i, null, i, i, null);
      builder.baselineValues(i, i, null, (double)i, (double)i, i);
      // match float with int and float
      builder.baselineValues((double)i, (double)i, i, i, i, null);
      builder.baselineValues((double)i, (double)i, i, (double)i, (double)i, i);
      // match string with string
      builder.baselineValues(stringVal, stringVal, null, stringVal, stringVal, null);
    }
    for (long i = 11; i <= 15; i++) {
      final String stringVal = Long.toString(i);
      builder.baselineValues(null, null, null, stringVal, stringVal, null);
    }
    builder.build().run();

    // OUTER
    builder = testBuilder()
      .sqlQuery(outerJoinQuery)
      .unOrdered()
      .optionSettingQueriesForTestQuery("alter session set `planner.enable_mergejoin` = false; alter session set `exec.enable_union_type` = true")
      .baselineColumns("lk", "lv", "lc", "rk", "rv", "rc");
    for (long i = 5; i <= 10; i++) {
      final String stringVal = Long.toString(i);
      builder.baselineValues(i, i, null, i, i, null);
      builder.baselineValues(i, i, null, (double)i, (double)i, i);
      builder.baselineValues((double)i, (double)i, i, i, i, null);
      builder.baselineValues((double)i, (double)i, i, (double)i, (double)i, i);
      builder.baselineValues(stringVal, stringVal, null, stringVal, stringVal, null);
    }
    for (long i = 1; i < 5; i++) {
      final String stringVal = Long.toString(i);
      builder.baselineValues(i, i, null, null, null, null);
      builder.baselineValues(stringVal, stringVal, null, null, null, null);
    }
    for (long i = 11; i <= 15; i++) {
      final String stringVal = Long.toString(i);
      builder.baselineValues((double)i, (double)i, i, null, null, null);
      builder.baselineValues(null, null, null, stringVal, stringVal, null);
    }
    builder.build().run();
  }

  @Test
  public void testOneSideSchemaChanges() throws Exception {
    final File left_dir = new File(BaseTestQuery.getTempDir("hashjoin-schemachanges-left"));
    final File right_dir = new File(BaseTestQuery.getTempDir("hashjoin-schemachanges-right"));
    left_dir.mkdirs();
    right_dir.mkdirs();
    System.out.println(left_dir);
    System.out.println(right_dir);

    BufferedWriter writer = new BufferedWriter(new FileWriter(new File(left_dir, "l1.json")));
    for (int i = 0; i < 50; ++i) {
      writer.write(String.format("{ \"kl\" : %d , \"vl\": %d }\n", i, i));
    }
    for (int i = 50; i < 100; ++i) {
      writer.write(String.format("{ \"kl\" : %f , \"vl\": %f }\n", (float) i, (float) i));
    }
    writer.close();

    writer = new BufferedWriter(new FileWriter(new File(right_dir, "r1.json")));
    for (int i = 0; i < 50; ++i) {
      writer.write(String.format("{ \"kl\" : %d , \"vl\": %d }\n", i, i));
    }
    writer.close();

    String query = String.format("select * from dfs_test.`%s` L %s join dfs_test.`%s` R on L.kl=R.kl",
      left_dir.toPath().toString(), "inner", right_dir.toPath().toString());
    TestBuilder builder = testBuilder()
      .sqlQuery(query)
      .optionSettingQueriesForTestQuery("alter session set `planner.enable_mergejoin` = false; alter session set `exec.enable_union_type` = true")
      .unOrdered()
      .baselineColumns("kl", "vl", "kl0", "vl0");

    for (long i = 0; i < 50; ++i) {
      builder.baselineValues(i, i, i, i);
    }
    builder.go();
  }

  @Test
  public void testMissingColumn() throws Exception {
    final File left_dir = new File(BaseTestQuery.getTempDir("hashjoin-schemachanges-left"));
    final File right_dir = new File(BaseTestQuery.getTempDir("hashjoin-schemachanges-right"));
    left_dir.mkdirs();
    right_dir.mkdirs();
    System.out.println(left_dir);
    System.out.println(right_dir);

    // missing column kl
    BufferedWriter writer = new BufferedWriter(new FileWriter(new File(left_dir, "l1.json")));
    for (int i = 0; i < 50; ++i) {
      writer.write(String.format("{ \"kl1\" : %d , \"vl1\": %d }\n", i, i));
    }
    writer.close();

    writer = new BufferedWriter(new FileWriter(new File(left_dir, "l2.json")));
    for (int i = 50; i < 100; ++i) {
      writer.write(String.format("{ \"kl\" : %d , \"vl\": %d }\n", i, i));
    }
    writer.close();

    writer = new BufferedWriter(new FileWriter(new File(left_dir, "l3.json")));
    for (int i = 100; i < 150; ++i) {
      writer.write(String.format("{ \"kl2\" : %d , \"vl2\": %d }\n", i, i));
    }
    writer.close();

    // right missing column kr
    writer = new BufferedWriter(new FileWriter(new File(right_dir, "r1.json")));
    for (int i = 0; i < 50; ++i) {
      writer.write(String.format("{ \"kr1\" : %f , \"vr1\": %f }\n", (float) i, (float) i));
    }
    //writer.write(String.format("{ \"kr1\" : null , \"vr1\": null , \"kr\" : 500 }\n"));
    //writer.write(String.format("{ \"kr1\" : null , \"vr1\": null , \"kr\" : 500.0 }\n"));
    writer.close();

    writer = new BufferedWriter(new FileWriter(new File(right_dir, "r2.json")));
    for (int i = 50; i < 100; ++i) {
      writer.write(String.format("{ \"kr\" : %f , \"vr\": %f }\n", (float)i, (float)i));
    }
    writer.close();

    writer = new BufferedWriter(new FileWriter(new File(right_dir, "r3.json")));
    for (int i = 100; i < 150; ++i) {
      writer.write(String.format("{ \"kr2\" : %f , \"vr2\": %f }\n", (float)i, (float)i));
    }
    writer.close();

    // INNER JOIN
    String query = String.format("select L.kl, L.vl, R.kr, R.vr, L.kl1, L.vl1, L.kl2, L.vl2, R.kr1, R.vr1, R.kr2, R.vr2 from dfs_test.`%s` L %s join dfs_test.`%s` R on L.kl=R.kr",
      left_dir.toPath().toString(), "inner", right_dir.toPath().toString());

    TestBuilder builder = testBuilder()
      .sqlQuery(query)
      .optionSettingQueriesForTestQuery("alter session set `planner.enable_mergejoin` = false; alter session set `exec.enable_union_type` = true")
      .unOrdered()
      .baselineColumns("kl", "vl", "kr", "vr", "kl1", "vl1", "kl2", "vl2", "kr1", "vr1", "kr2", "vr2");

    for (long i = 50; i < 100; ++i) {
      builder.baselineValues(i, i, (double)i, (double)i, null, null, null, null, null, null, null, null);
    }
    builder.go();
    /*
    // LEFT JOIN
    query = String.format("select * from dfs_test.`%s` L %s join dfs_test.`%s` R on L.kl=R.kr",
      left_dir.toPath().toString(), "left", right_dir.toPath().toString());

    builder = testBuilder()
      .sqlQuery(query)
      .optionSettingQueriesForTestQuery("alter session set `planner.enable_hashjoin` = false; alter session set `exec.enable_union_type` = true")
      .unOrdered()
      .baselineColumns("kl", "vl", "kr", "vr", "kl1", "vl1", "kl2", "vl2", "kr1", "vr1", "kr2", "vr2");

    for (long i = 0; i < 50; ++i) {
      builder.baselineValues(null, null, null, null, i, i, null, null, null, null, null, null);
    }
    for (long i = 50; i < 100; ++i) {
      builder.baselineValues(i, i, (double)i, (double)i, null, null, null, null, null, null, null, null);
    }
    for (long i = 100; i < 150; ++i) {
      builder.baselineValues(null, null, null, null, null, null, i, i, null, null, null, null);
    }
    builder.go();

    // RIGHT JOIN
    query = String.format("select * from dfs_test.`%s` L %s join dfs_test.`%s` R on L.kl=R.kr",
      left_dir.toPath().toString(), "right", right_dir.toPath().toString());

    builder = testBuilder()
      .sqlQuery(query)
      .optionSettingQueriesForTestQuery("alter session set `planner.enable_hashjoin` = false; alter session set `exec.enable_union_type` = true")
      .unOrdered()
      .baselineColumns("kl", "vl", "kr", "vr", "kl1", "vl1", "kl2", "vl2", "kr1", "vr1", "kr2", "vr2");

    for (long i = 0; i < 50; ++i) {
      builder.baselineValues(null, null, null, null, null, null, null, null, (double)i, (double)i, null, null);
    }
    for (long i = 50; i < 100; ++i) {
      builder.baselineValues(i, i, (double)i, (double)i, null, null, null, null, null, null, null, null);
    }
    for (long i = 100; i < 150; ++i) {
      builder.baselineValues(null, null, null, null, null, null, null, null, null, null, (double)i, (double)i);
    }
    builder.go();
    */
  }
}
