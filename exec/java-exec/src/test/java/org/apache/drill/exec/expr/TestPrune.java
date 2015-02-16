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
package org.apache.drill.exec.expr;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.util.TestTools;
import org.junit.Test;

public class TestPrune extends BaseTestQuery {

  String MULTILEVEL = TestTools.getWorkingPath() + "/../java-exec/src/test/resources/multilevel";

  @Test
  public void pruneCompound1() throws Exception {
    test(String.format("select * from dfs.`%s/csv` where x is null and dir1 in ('Q1', 'Q2')", MULTILEVEL));
  }

  @Test
  public void pruneSimple1() throws Exception {
    test(String.format("select * from dfs.`%s/csv` where dir1 in ('Q1', 'Q2')", MULTILEVEL));
  }

  @Test
  public void pruneCompound2() throws Exception {
    String query1 = String.format("select * from dfs_test.`%s/parquet` where (dir0=1995 and o_totalprice < 40000) or (dir0=1996 and o_totalprice < 40000)", MULTILEVEL);
    String query2 = String.format("select * from dfs_test.`%s/parquet` where dir0=1995 and o_totalprice < 40000", MULTILEVEL);
    String query3 = String.format("select * from dfs_test.`%s/parquet` where (dir0=1995 and o_totalprice < 40000) or dir0=1996", MULTILEVEL);
    String query4 = String.format("select * from dfs_test.`%s/parquet` where dir0=1995 or dir0=1996", MULTILEVEL);
    test(query3);
  }

}
