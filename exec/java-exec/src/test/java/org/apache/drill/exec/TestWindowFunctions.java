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
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.work.foreman.SqlUnsupportedException;
import org.apache.drill.exec.work.foreman.UnsupportedFunctionException;
import org.junit.Test;

public class TestWindowFunctions extends BaseTestQuery {
  private static void throwAsUnsupportedException(UserException ex) throws Exception {
    SqlUnsupportedException.errorClassNameToException(ex.getOrCreatePBError(false).getException().getExceptionClass());
    throw ex;
  }

  @Test // DRILL-3196
  public void testSinglePartition() throws Exception {
    final String query = "explain plan for select sum(a2) over(partition by a2), count(*) over(partition by a2) \n" +
        "from cp.`tpch/nation.parquet`";

    test("alter session set `window.enable` = true");
    test(query);
    test("alter session set `window.enable` = false");
  }

  @Test // DRILL-3196
  public void testSinglePartitionDefinedInWindowList() throws Exception {
    final String query = "explain plan for select sum(a2) over w \n" +
        "from cp.`tpch/nation.parquet` \n" +
        "window w as (partition by a2 order by a2)";

    test("alter session set `window.enable` = true");
    test(query);
    test("alter session set `window.enable` = false");
  }

  @Test(expected = UnsupportedFunctionException.class) // DRILL-3196
  public void testMultiplePartitions() throws Exception {
    try {
      final String query = "explain plan for select sum(a2) over(partition by a2), count(*) over(partition by a2,b2,c2) \n" +
          "from cp.`tpch/nation.parquet`";

      test("alter session set `window.enable` = true");
      test(query);
      test("alter session set `window.enable` = false");
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // DRILL-3196
  public void testSinglePartitionMultipleOrderBy() throws Exception {
    try {
      final String query = "explain plan for select sum(a2) over(partition by a2 order by a2), count(*) over(partition by a2 order by b2) \n" +
          "from cp.`tpch/nation.parquet`";

      test("alter session set `window.enable` = true");
      test(query);
      test("alter session set `window.enable` = false");
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // DRILL-3196
  public void testMultiplePartitionsDefinedInWindowList() throws Exception {
    try {
      final String query = "explain plan for select sum(a2) over(partition by a2), count(*) over w \n" +
          "from cp.`tpch/nation.parquet` \n" +
          "window w as (partition by a2, b2, c2)";

      test("alter session set `window.enable` = true");
      test(query);
      test("alter session set `window.enable` = false");
    } catch(UserException ex) {
      throwAsUnsupportedException(ex);
      throw ex;
    }
  }
}
