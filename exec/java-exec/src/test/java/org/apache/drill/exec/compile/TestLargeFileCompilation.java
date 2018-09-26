/*
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
package org.apache.drill.exec.compile;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.drill.categories.SlowTest;
import org.apache.drill.test.BaseTestQuery;
import org.apache.drill.test.TestTools;
import org.apache.drill.exec.ExecConstants;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;

@Category({SlowTest.class})
public class TestLargeFileCompilation extends BaseTestQuery {
  @Rule public final TestRule TIMEOUT = TestTools.getTimeoutRule(200000); // 200 secs

  private static final String LARGE_QUERY_GROUP_BY;

  private static final String LARGE_QUERY_ORDER_BY;

  private static final String LARGE_QUERY_ORDER_BY_WITH_LIMIT;

  private static final String LARGE_QUERY_FILTER;

  private static final String HUGE_STRING_CONST_QUERY;

  private static final String LARGE_QUERY_WRITER;

  private static final String LARGE_QUERY_SELECT_LIST;

  private static final String QUERY_WITH_JOIN;

  private static final String LARGE_TABLE_WRITER;

  private static final int ITERATION_COUNT = Integer.valueOf(System.getProperty("TestLargeFileCompilation.iteration", "1"));

  private static final int NUM_PROJECT_COLUMNS = 2500;

  private static final int NUM_PROJECT_TEST_COLUMNS = 5000;

  private static final int NUM_ORDERBY_COLUMNS = 500;

  private static final int NUM_GROUPBY_COLUMNS = 225;

  private static final int NUM_FILTER_COLUMNS = 150;

  private static final int NUM_JOIN_TABLE_COLUMNS = 500;

  static {
    StringBuilder sb = new StringBuilder("select\n\t");
    for (int i = 0; i < NUM_GROUPBY_COLUMNS; i++) {
      sb.append("c").append(i).append(", ");
    }
    sb.append("full_name\nfrom (select\n\t");
    for (int i = 0; i < NUM_GROUPBY_COLUMNS; i++) {
      sb.append("employee_id+").append(i).append(" as c").append(i).append(", ");
    }
    sb.append("full_name\nfrom cp.`employee.json`)\ngroup by\n\t");
    for (int i = 0; i < NUM_GROUPBY_COLUMNS; i++) {
      sb.append("c").append(i).append(", ");
    }
    LARGE_QUERY_GROUP_BY = sb.append("full_name").toString();
  }

  static {
    StringBuilder sb = new StringBuilder("select\n\t");
    for (int i = 0; i < NUM_PROJECT_TEST_COLUMNS; i++) {
      sb.append("employee_id+").append(i).append(" as col").append(i).append(", ");
    }
    sb.append("full_name\nfrom cp.`employee.json`\n\n\t");
    LARGE_QUERY_SELECT_LIST = sb.append("full_name").toString();
  }

  static {
    StringBuilder sb = new StringBuilder("select\n\t");
    for (int i = 0; i < NUM_PROJECT_COLUMNS; i++) {
      sb.append("employee_id+").append(i).append(" as col").append(i).append(", ");
    }
    sb.append("full_name\nfrom cp.`employee.json`\norder by\n\t");
    for (int i = 0; i < NUM_ORDERBY_COLUMNS; i++) {
      sb.append(" col").append(i).append(", ");
    }
    LARGE_QUERY_ORDER_BY = sb.append("full_name").toString();
    LARGE_QUERY_ORDER_BY_WITH_LIMIT = sb.append("\nlimit 1").toString();
  }

  static {
    StringBuilder sb = new StringBuilder("select *\n")
      .append("from cp.`employee.json`\n")
      .append("where");
    for (int i = 0; i < NUM_FILTER_COLUMNS; i++) {
      sb.append(" employee_id+").append(i).append(" < employee_id ").append(i%2==0?"OR":"AND");
    }
    LARGE_QUERY_FILTER = sb.append(" true") .toString();
  }

  static {
    final char[] alphabet = "abcdefghijklmnopqrstuvwxyz".toCharArray();
    int len = 1 << 18;
    char[] longText = new char[len];
    for (int j = 0; j < len; ++j) {
      longText[j] = alphabet[ThreadLocalRandom.current().nextInt(0, alphabet.length)];
    }
    StringBuilder sb = new StringBuilder("select *\n")
      .append("from cp.`employee.json`\n")
      .append("where last_name ='")
      .append(longText)
      .append("'");
    HUGE_STRING_CONST_QUERY = sb.toString();
  }

  static {
    LARGE_QUERY_WRITER = createTableWithColsCount(NUM_PROJECT_COLUMNS);
    LARGE_TABLE_WRITER = createTableWithColsCount(NUM_JOIN_TABLE_COLUMNS);
    QUERY_WITH_JOIN = "select * from %1$s t1, %1$s t2 where t1.col1 = t2.col1";
  }

  private static String createTableWithColsCount(int columnsCount) {
    StringBuilder sb = new StringBuilder("create table %s as (select \n");
    for (int i = 0; i < columnsCount; i++) {
      sb.append("employee_id+").append(i).append(" as col").append(i).append(", ");
    }
    return sb.append("full_name\nfrom cp.`employee.json` limit 1)").toString();
  }

  @Ignore // TODO DRILL-5997
  @Test
  public void testTEXT_WRITER() throws Exception {
    testNoResult("alter session set `%s`='JDK'", ClassCompilerSelector.JAVA_COMPILER_OPTION);
    testNoResult("use dfs.tmp");
    testNoResult("alter session set `%s`='csv'", ExecConstants.OUTPUT_FORMAT_OPTION);
    testNoResult(LARGE_QUERY_WRITER, "wide_table_csv");
  }

  @Test
  public void testPARQUET_WRITER() throws Exception {
    testNoResult("alter session set `%s`='JDK'", ClassCompilerSelector.JAVA_COMPILER_OPTION);
    testNoResult("use dfs.tmp");
    testNoResult("alter session set `%s`='parquet'", ExecConstants.OUTPUT_FORMAT_OPTION);
    testNoResult(ITERATION_COUNT, LARGE_QUERY_WRITER, "wide_table_parquet");
  }

  @Ignore // TODO DRILL-5997
  @Test
  public void testGROUP_BY() throws Exception {
    testNoResult("alter session set `%s`='JDK'", ClassCompilerSelector.JAVA_COMPILER_OPTION);
    testNoResult(ITERATION_COUNT, LARGE_QUERY_GROUP_BY);
  }

  @Test
  public void testEXTERNAL_SORT() throws Exception {
    testNoResult("alter session set `%s`='JDK'", ClassCompilerSelector.JAVA_COMPILER_OPTION);
    testNoResult(ITERATION_COUNT, LARGE_QUERY_ORDER_BY);
  }

  @Test
  public void testTOP_N_SORT() throws Exception {
    testNoResult("alter session set `%s`='JDK'", ClassCompilerSelector.JAVA_COMPILER_OPTION);
    testNoResult(ITERATION_COUNT, LARGE_QUERY_ORDER_BY_WITH_LIMIT);
  }

  @Ignore // TODO DRILL-5997
  @Test
  public void testFILTER() throws Exception {
    testNoResult("alter session set `%s`='JDK'", ClassCompilerSelector.JAVA_COMPILER_OPTION);
    testNoResult(ITERATION_COUNT, LARGE_QUERY_FILTER);
  }

  @Test
  public void testProject() throws Exception {
    testNoResult("alter session set `%s`='JDK'", ClassCompilerSelector.JAVA_COMPILER_OPTION);
    testNoResult(ITERATION_COUNT, LARGE_QUERY_SELECT_LIST);
  }

  @Test
  public void testHashJoin() throws Exception {
    String tableName = "wide_table_hash_join";
    try {
      setSessionOption("drill.exec.hashjoin.fallback.enabled", true);
      testNoResult("alter session set `%s`='JDK'", ClassCompilerSelector.JAVA_COMPILER_OPTION);
      testNoResult("alter session set `planner.enable_mergejoin` = false");
      testNoResult("alter session set `planner.enable_nestedloopjoin` = false");
      testNoResult("use dfs.tmp");
      testNoResult(LARGE_TABLE_WRITER, tableName);
      testNoResult(QUERY_WITH_JOIN, tableName);
    } finally {
      testNoResult("alter session reset `planner.enable_mergejoin`");
      testNoResult("alter session reset `planner.enable_nestedloopjoin`");
      testNoResult("alter session reset `%s`", ClassCompilerSelector.JAVA_COMPILER_OPTION);
      testNoResult("drop table if exists %s", tableName);
    }
  }

  @Test
  public void testMergeJoin() throws Exception {
    String tableName = "wide_table_merge_join";
    try {
      testNoResult("alter session set `%s`='JDK'", ClassCompilerSelector.JAVA_COMPILER_OPTION);
      testNoResult("alter session set `planner.enable_hashjoin` = false");
      testNoResult("alter session set `planner.enable_nestedloopjoin` = false");
      testNoResult("use dfs.tmp");
      testNoResult(LARGE_TABLE_WRITER, tableName);
      testNoResult(QUERY_WITH_JOIN, tableName);
    } finally {
      testNoResult("alter session reset `planner.enable_hashjoin`");
      testNoResult("alter session reset `planner.enable_nestedloopjoin`");
      testNoResult("alter session reset `%s`", ClassCompilerSelector.JAVA_COMPILER_OPTION);
      testNoResult("drop table if exists %s", tableName);
    }
  }

  @Test
  public void testNestedLoopJoin() throws Exception {
    String tableName = "wide_table_loop_join";
    try {
      testNoResult("alter session set `%s`='JDK'", ClassCompilerSelector.JAVA_COMPILER_OPTION);
      testNoResult("alter session set `planner.enable_nljoin_for_scalar_only` = false");
      testNoResult("alter session set `planner.enable_hashjoin` = false");
      testNoResult("alter session set `planner.enable_mergejoin` = false");
      testNoResult("use dfs.tmp");
      testNoResult(LARGE_TABLE_WRITER, tableName);
      testNoResult(QUERY_WITH_JOIN, tableName);
    } finally {
      testNoResult("alter session reset `planner.enable_nljoin_for_scalar_only`");
      testNoResult("alter session reset `planner.enable_hashjoin`");
      testNoResult("alter session reset `planner.enable_mergejoin`");
      testNoResult("alter session reset `%s`", ClassCompilerSelector.JAVA_COMPILER_OPTION);
      testNoResult("drop table if exists %s", tableName);
    }
  }

  @Test
  public void testJDKHugeStringConstantCompilation() throws Exception {
    try {
      setSessionOption(ClassCompilerSelector.JAVA_COMPILER_OPTION, "JDK");
      testNoResult(ITERATION_COUNT, HUGE_STRING_CONST_QUERY);
    } finally {
      resetSessionOption(ClassCompilerSelector.JAVA_COMPILER_OPTION);
    }
  }

  @Test
  public void testJaninoHugeStringConstantCompilation() throws Exception {
    try {
      setSessionOption(ClassCompilerSelector.JAVA_COMPILER_OPTION, "JANINO");
      testNoResult(ITERATION_COUNT, HUGE_STRING_CONST_QUERY);
    } finally {
      resetSessionOption(ClassCompilerSelector.JAVA_COMPILER_OPTION);
    }
  }
}
