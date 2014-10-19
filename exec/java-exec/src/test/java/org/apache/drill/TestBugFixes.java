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
package org.apache.drill;

import org.apache.drill.exec.rpc.RpcException;
import org.junit.Ignore;
import org.junit.Test;

public class TestBugFixes extends BaseTestQuery {

  @Test
  public void leak1() throws Exception {
    String select = "select count(*) \n" +
        "    from cp.`tpch/part.parquet` p1, cp.`tpch/part.parquet` p2 \n" +
        "    where p1.p_name = p2.p_name \n" +
        "  and p1.p_mfgr = p2.p_mfgr";
    test(select);
  }

  @Ignore
  @Test
  public void failingSmoke() throws Exception {
    String select = "select count(*) \n" +
        "  from (select l.l_orderkey as x, c.c_custkey as y \n" +
        "  from cp.`tpch/lineitem.parquet` l \n" +
        "    left outer join cp.`tpch/customer.parquet` c \n" +
        "      on l.l_orderkey = c.c_custkey) as foo\n" +
        "  where x < 10000";
    test(select);
  }

  @Test
  public void testSysDrillbits() throws Exception {
    test("select * from sys.drillbits");
  }

  @Test
  public void testVersionTable() throws Exception {
    test("select * from sys.version");
  }

  @Test
  public void DRILL883() throws Exception {
    test("select n1.n_regionkey from cp.`tpch/nation.parquet` n1, (select n_nationkey from cp.`tpch/nation.parquet`) as n2 where n1.n_nationkey = n2.n_nationkey");
  }

  @Test
  public void DRILL1061() throws Exception {
    String query = "select foo.mycol.x as COMPLEX_COL from (select convert_from('{ x : [1,2], y : 100 }', 'JSON') as mycol from cp.`tpch/nation.parquet`) as foo(mycol) limit 1";
    test(query);
  }

  @Test
  public void DRILL1126() throws Exception {
    String query = "select sum(cast(employee_id as decimal(38, 18))), avg(cast(employee_id as decimal(38, 18))) from cp.`employee.json` group by (department_id)";
    test(query);
  }


  @Test (expected = RpcException.class)
  // Should be "Failure while parsing sql. Node [rel#26:Subset#6.LOGICAL.ANY([]).[]] could not be implemented;".
  // Drill will hit CanNotPlan, until we add code fix to transform the local LHS filter in left outer join properly.
  public void testDRILL1337_LocalLeftFilterLeftOutJoin() throws Exception {
    try {
      test("select count(*) from cp.`tpch/nation.parquet` n left outer join cp.`tpch/region.parquet` r on n.n_regionkey = r.r_regionkey and n.n_nationkey > 10;");
    } catch (RpcException e) {
      logger.info("***** Test resulted in expected failure: " + e.getMessage());
      throw e;
    }
  }

  @Test
  public void testDRILL1337_LocalRightFilterLeftOutJoin() throws Exception {
    test("select * from cp.`tpch/nation.parquet` n left outer join cp.`tpch/region.parquet` r on n.n_regionkey = r.r_regionkey and r.r_name not like '%ASIA' order by r.r_name;");
  }

}
