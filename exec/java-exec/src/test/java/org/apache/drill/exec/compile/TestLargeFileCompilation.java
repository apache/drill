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
package org.apache.drill.exec.compile;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.ExecConstants;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class TestLargeFileCompilation extends BaseTestQuery {
  @Rule public final TestRule TIMEOUT = TestTools.getTimeoutRule(150000); // 150secs

  private static final String LARGE_QUERY_GROUP_BY;

  private static final String LARGE_QUERY_ORDER_BY;

  private static final String LARGE_QUERY_ORDER_BY_WITH_LIMIT;

  private static final String LARGE_QUERY_FILTER;

  private static final String LARGE_QUERY_WRITER;

  private static final String LARGE_QUERY_SELECT_LIST;

  private static final int ITERATION_COUNT = Integer.valueOf(System.getProperty("TestLargeFileCompilation.iteration", "1"));

  private static final int NUM_PROJECT_COULMNS = 2000;

  private static final int NUM_ORDERBY_COULMNS = 500;

  private static final int NUM_GROUPBY_COULMNS = 225;

  private static final int NUM_FILTER_COULMNS = 150;

  static {
    StringBuilder sb = new StringBuilder("select\n\t");
    for (int i = 0; i < NUM_GROUPBY_COULMNS; i++) {
      sb.append("c").append(i).append(", ");
    }
    sb.append("full_name\nfrom (select\n\t");
    for (int i = 0; i < NUM_GROUPBY_COULMNS; i++) {
      sb.append("employee_id+").append(i).append(" as c").append(i).append(", ");
    }
    sb.append("full_name\nfrom cp.`employee.json`)\ngroup by\n\t");
    for (int i = 0; i < NUM_GROUPBY_COULMNS; i++) {
      sb.append("c").append(i).append(", ");
    }
    LARGE_QUERY_GROUP_BY = sb.append("full_name").toString();
  }

  static {
    StringBuilder sb = new StringBuilder("select\n\t");
    for (int i = 0; i < NUM_PROJECT_COULMNS; i++) {
      sb.append("employee_id+").append(i).append(" as col").append(i).append(", ");
    }
    sb.append("full_name\nfrom cp.`employee.json`\n\n\t");
    LARGE_QUERY_SELECT_LIST = sb.append("full_name").toString();
  }

  static {
    StringBuilder sb = new StringBuilder("select\n\t");
    for (int i = 0; i < NUM_PROJECT_COULMNS; i++) {
      sb.append("employee_id+").append(i).append(" as col").append(i).append(", ");
    }
    sb.append("full_name\nfrom cp.`employee.json`\norder by\n\t");
    for (int i = 0; i < NUM_ORDERBY_COULMNS; i++) {
      sb.append(" col").append(i).append(", ");
    }
    LARGE_QUERY_ORDER_BY = sb.append("full_name").toString();
    LARGE_QUERY_ORDER_BY_WITH_LIMIT = sb.append("\nlimit 1").toString();
  }

  static {
    StringBuilder sb = new StringBuilder("select *\n")
      .append("from cp.`employee.json`\n")
      .append("where");
    for (int i = 0; i < NUM_FILTER_COULMNS; i++) {
      sb.append(" employee_id+").append(i).append(" < employee_id ").append(i%2==0?"OR":"AND");
    }
    LARGE_QUERY_FILTER = sb.append(" true") .toString();
  }

  static {
    StringBuilder sb = new StringBuilder("create table %s as (select \n");
    for (int i = 0; i < NUM_PROJECT_COULMNS; i++) {
      sb.append("employee_id+").append(i).append(" as col").append(i).append(", ");
    }
    LARGE_QUERY_WRITER = sb.append("full_name\nfrom cp.`employee.json` limit 1)").toString();
  }

  @Test
  public void testTEXT_WRITER() throws Exception {
    testNoResult("alter session set `%s`='JDK'", QueryClassLoader.JAVA_COMPILER_OPTION);
    testNoResult("use dfs_test.tmp");
    testNoResult("alter session set `%s`='csv'", ExecConstants.OUTPUT_FORMAT_OPTION);
    testNoResult(LARGE_QUERY_WRITER, "wide_table_csv");
  }

  @Test
  public void testPARQUET_WRITER() throws Exception {
    testNoResult("alter session set `%s`='JDK'", QueryClassLoader.JAVA_COMPILER_OPTION);
    testNoResult("use dfs_test.tmp");
    testNoResult("alter session set `%s`='parquet'", ExecConstants.OUTPUT_FORMAT_OPTION);
    testNoResult(ITERATION_COUNT, LARGE_QUERY_WRITER, "wide_table_parquet");
  }

  @Test
  public void testGROUP_BY() throws Exception {
    testNoResult("alter session set `%s`='JDK'", QueryClassLoader.JAVA_COMPILER_OPTION);
    testNoResult(ITERATION_COUNT, LARGE_QUERY_GROUP_BY);
  }

  @Test
  @Ignore("DRILL-1808")
  public void testEXTERNAL_SORT() throws Exception {
    testNoResult("alter session set `%s`='JDK'", QueryClassLoader.JAVA_COMPILER_OPTION);
    testNoResult(ITERATION_COUNT, LARGE_QUERY_ORDER_BY);
  }

  @Test
  @Ignore("DRILL-1808")
  public void testTOP_N_SORT() throws Exception {
    testNoResult("alter session set `%s`='JDK'", QueryClassLoader.JAVA_COMPILER_OPTION);
    testNoResult(ITERATION_COUNT, LARGE_QUERY_ORDER_BY_WITH_LIMIT);
  }

  @Test
  public void testFILTER() throws Exception {
    testNoResult("alter session set `%s`='JDK'", QueryClassLoader.JAVA_COMPILER_OPTION);
    testNoResult(ITERATION_COUNT, LARGE_QUERY_FILTER);
  }

  @Test
  public void testProject() throws Exception {
    testNoResult("alter session set `%s`='JDK'", QueryClassLoader.JAVA_COMPILER_OPTION);
    testNoResult(ITERATION_COUNT, LARGE_QUERY_SELECT_LIST);
  }

}
