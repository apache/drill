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
package org.apache.drill.jdbc.test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.apache.drill.exec.ref.ReferenceInterpreter;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.io.Resources;

/** Unit tests for Drill's JDBC driver. */

public class JdbcTest {
  private static String MODEL;
  private static String EXPECTED;


  @BeforeClass
  public static void setupFixtures() throws IOException {
    MODEL = Resources.toString(Resources.getResource("test-models.json"), Charsets.UTF_8);
    EXPECTED = Resources.toString(Resources.getResource("donuts-output-data.txt"), Charsets.UTF_8);
  }

  /**
   * Command-line utility to execute a logical plan.
   * 
   * <p>
   * The forwarding method ensures that the IDE calls this method with the right classpath.
   * </p>
   */
  public static void main(String[] args) throws Exception {
    ReferenceInterpreter.main(args);
  }

  /** Load driver. */
  @Test
  public void testLoadDriver() throws ClassNotFoundException {
    Class.forName("org.apache.drill.jdbc.Driver");
  }

  /** Load driver and make a connection. */
  @Test
  public void testConnect() throws Exception {
    Class.forName("org.apache.drill.jdbc.Driver");
    final Connection connection = DriverManager.getConnection("jdbc:drillref:schema=DONUTS");
    connection.close();
  }

  /** Load driver, make a connection, prepare a statement. */
  @Test
  public void testPrepare() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS").withConnection(new Function<Connection, Void>() {
      public Void apply(Connection connection) {
        try {
          final Statement statement = connection.prepareStatement("select * from donuts");
          statement.close();
          return null;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  /** Simple query against JSON. */
  @Test
  public void testSelectJson() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS").sql("select * from donuts").returns(EXPECTED);
  }

  /** Simple query against EMP table in HR database. */
  @Test
  public void testSelectEmployees() throws Exception {
    JdbcAssert
        .withModel(MODEL, "HR")
        .sql("select * from employees")
        .returns(
            "_MAP={deptId=31, lastName=Rafferty}\n" + "_MAP={deptId=33, lastName=Jones}\n"
                + "_MAP={deptId=33, lastName=Steinberg}\n" + "_MAP={deptId=34, lastName=Robinson}\n"
                + "_MAP={deptId=34, lastName=Smith}\n" + "_MAP={lastName=John}\n");
  }

  /** Simple query against EMP table in HR database. */
  @Test
  public void testSelectEmpView() throws Exception {
    JdbcAssert
        .withModel(MODEL, "HR")
        .sql("select * from emp")
        .returns(
            "DEPTID=31; LASTNAME=Rafferty\n" + "DEPTID=33; LASTNAME=Jones\n" + "DEPTID=33; LASTNAME=Steinberg\n"
                + "DEPTID=34; LASTNAME=Robinson\n" + "DEPTID=34; LASTNAME=Smith\n" + "DEPTID=null; LASTNAME=John\n");
  }

  /** Simple query against EMP table in HR database. */
  @Test
  public void testSelectDept() throws Exception {
    JdbcAssert
        .withModel(MODEL, "HR")
        .sql("select * from departments")
        .returns(
            "_MAP={deptId=31, name=Sales}\n" + "_MAP={deptId=33, name=Engineering}\n"
                + "_MAP={deptId=34, name=Clerical}\n" + "_MAP={deptId=35, name=Marketing}\n");
  }

  /** Query with project list. No field references yet. */
  @Test
  public void testProjectConstant() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS").sql("select 1 + 3 as c from donuts")
        .returns("C=4\n" + "C=4\n" + "C=4\n" + "C=4\n" + "C=4\n");
  }

  /** Query that projects an element from the map. */
  @Test
  public void testProject() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS").sql("select _MAP['ppu'] as ppu from donuts")
        .returns("PPU=0.55\n" + "PPU=0.69\n" + "PPU=0.55\n" + "PPU=0.69\n" + "PPU=1.0\n");
  }

  /** Same logic as {@link #testProject()}, but using a subquery. */
  @Test
  public void testProjectOnSubquery() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS").sql("select d['ppu'] as ppu from (\n" + " select _MAP as d from donuts)")
        .returns("PPU=0.55\n" + "PPU=0.69\n" + "PPU=0.55\n" + "PPU=0.69\n" + "PPU=1.0\n");
  }

  /** Checks the logical plan. */
  @Test
  public void testProjectPlan() throws Exception {
    JdbcAssert
        .withModel(MODEL, "DONUTS")
        .sql("select _MAP['ppu'] as ppu from donuts")
        .planContains(
            "{'head':{'type':'APACHE_DRILL_LOGICAL','version':'1','generator':{'type':'optiq','info':'na'}},"
                + "'storage':{'donuts-json':{'type':'classpath'},'queue':{'type':'queue'}},"
                + "'query':["
                + "{'op':'scan','memo':'initial_scan','ref':'_MAP','storageengine':'donuts-json','selection':{'path':'/donuts.json','type':'JSON'},'@id':1},"
                + "{'op':'project','input':1,'projections':[{'expr':'_MAP.ppu','ref':'output.PPU'}],'@id':2},"
                + "{'op':'store','input':2,'storageengine':'queue','memo':'output sink','target':{'number':0},'@id':3}]}");
  }

  /**
   * Query with subquery, filter, and projection of one real and one nonexistent field from a map field.
   */
  @Test
  public void testProjectFilterSubquery() throws Exception {
    JdbcAssert
        .withModel(MODEL, "DONUTS")
        .sql(
            "select d['name'] as name, d['xx'] as xx from (\n" + " select _MAP as d from donuts)\n"
                + "where cast(d['ppu'] as double) > 0.6")
        .returns("NAME=Raised; XX=null\n" + "NAME=Filled; XX=null\n" + "NAME=Apple Fritter; XX=null\n");
  }

  @Test
  public void testProjectFilterSubqueryPlan() throws Exception {
    JdbcAssert
        .withModel(MODEL, "DONUTS")
        .sql(
            "select d['name'] as name, d['xx'] as xx from (\n" + " select _MAP['donuts'] as d from donuts)\n"
                + "where cast(d['ppu'] as double) > 0.6")
        .planContains(
            "{'head':{'type':'APACHE_DRILL_LOGICAL','version':'1','generator':{'type':'optiq','info':'na'}},'storage':{'donuts-json':{'type':'classpath'},'queue':{'type':'queue'}},"
                + "'query':["
                + "{'op':'scan','memo':'initial_scan','ref':'_MAP','storageengine':'donuts-json','selection':{'path':'/donuts.json','type':'JSON'},'@id':1},"
                + "{'op':'filter','input':1,'expr':'(_MAP.donuts.ppu > 0.6)','@id':2},"
                + "{'op':'project','input':2,'projections':[{'expr':'_MAP.donuts','ref':'output.D'}],'@id':3},"
                + "{'op':'project','input':3,'projections':[{'expr':'D.name','ref':'output.NAME'},{'expr':'D.xx','ref':'output.XX'}],'@id':4},"
                + "{'op':'store','input':4,'storageengine':'queue','memo':'output sink','target':{'number':0},'@id':5}]}");
  }

  /** Query that projects one field. (Disabled; uses sugared syntax.) */
  @Test @Ignore
  public void testProjectNestedFieldSugared() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS").sql("select donuts.ppu from donuts")
        .returns("C=4\n" + "C=4\n" + "C=4\n" + "C=4\n" + "C=4\n");
  }

  /** Query with filter. No field references yet. */
  @Test
  public void testFilterConstantFalse() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS").sql("select * from donuts where 3 > 4").returns("");
  }

  @Test
  public void testFilterConstant() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS").sql("select * from donuts where 3 < 4").returns(EXPECTED);
  }

  @Test
  public void testValues() throws Exception {
    JdbcAssert.withModel(MODEL, "DONUTS").sql("values (1)").returns("EXPR$0=1\n");

    // Enable when https://issues.apache.org/jira/browse/DRILL-57 fixed
    // .planContains("store");
  }

  @Test
  public void testDistinct() throws Exception {
    JdbcAssert.withModel(MODEL, "HR").sql("select distinct deptId from emp")
        .returnsUnordered("DEPTID=null", "DEPTID=31", "DEPTID=34", "DEPTID=33")
        .planContains("\"op\":\"collapsingaggregate\"");
  }

  @Test
  public void testCountNoGroupBy() throws Exception {
    // 5 out of 6 employees have a not-null deptId
    JdbcAssert.withModel(MODEL, "HR").sql("select count(deptId) as cd, count(*) as c from emp").returns("CD=5; C=6\n")
        .planContains("\"op\":\"collapsingaggregate\"");
  }

  @Test
  public void testDistinctCountNoGroupBy() throws Exception {
    JdbcAssert.withModel(MODEL, "HR").sql("select count(distinct deptId) as c from emp").returns("C=3\n")
        .planContains("\"op\":\"collapsingaggregate\"");
  }

  @Test
  public void testDistinctCountGroupByEmpty() throws Exception {
    JdbcAssert.withModel(MODEL, "HR").sql("select count(distinct deptId) as c from emp group by ()").returns("C=3\n")
        .planContains("\"op\":\"collapsingaggregate\"");
  }

  @Test
  public void testCountNull() throws Exception {
    JdbcAssert.withModel(MODEL, "HR").sql("select count(distinct deptId) as c from emp group by ()").returns("C=3\n")
        .planContains("\"op\":\"collapsingaggregate\"");
  }

  @Test
  public void testCount() throws Exception {
    JdbcAssert.withModel(MODEL, "HR").sql("select deptId, count(*) as c from emp group by deptId")
        .returnsUnordered("DEPTID=31; C=1", "DEPTID=33; C=2", "DEPTID=34; C=2", "DEPTID=null; C=1")
        .planContains("\"op\":\"collapsingaggregate\""); // make sure using drill
  }

  @Test
  public void testJoin() throws Exception {
    JdbcAssert
        .withModel(MODEL, "HR")
        .sql("select * from emp join dept on emp.deptId = dept.deptId")
        .returnsUnordered("DEPTID=31; LASTNAME=Rafferty; DEPTID0=31; NAME=Sales",
            "DEPTID=33; LASTNAME=Jones; DEPTID0=33; NAME=Engineering",
            "DEPTID=33; LASTNAME=Steinberg; DEPTID0=33; NAME=Engineering",
            "DEPTID=34; LASTNAME=Robinson; DEPTID0=34; NAME=Clerical",
            "DEPTID=34; LASTNAME=Smith; DEPTID0=34; NAME=Clerical").planContains("'type':'inner'");
  }

  @Test
  public void testLeftJoin() throws Exception {
    JdbcAssert
        .withModel(MODEL, "HR")
        .sql("select * from emp left join dept on emp.deptId = dept.deptId")
        .returnsUnordered("DEPTID=31; LASTNAME=Rafferty; DEPTID0=31; NAME=Sales",
            "DEPTID=33; LASTNAME=Jones; DEPTID0=33; NAME=Engineering",
            "DEPTID=33; LASTNAME=Steinberg; DEPTID0=33; NAME=Engineering",
            "DEPTID=34; LASTNAME=Robinson; DEPTID0=34; NAME=Clerical",
            "DEPTID=34; LASTNAME=Smith; DEPTID0=34; NAME=Clerical",
            "DEPTID=null; LASTNAME=John; DEPTID0=null; NAME=null").planContains("'type':'left'");
  }

  /**
   * Right join is tricky because Drill's "join" operator only supports "left", so we have to flip inputs.
   */
  @Test @Ignore
  public void testRightJoin() throws Exception {
    JdbcAssert.withModel(MODEL, "HR").sql("select * from emp right join dept on emp.deptId = dept.deptId")
        .returnsUnordered("xx").planContains("'type':'left'");
  }

  @Test
  public void testFullJoin() throws Exception {
    JdbcAssert
        .withModel(MODEL, "HR")
        .sql("select * from emp full join dept on emp.deptId = dept.deptId")
        .returnsUnordered("DEPTID=31; LASTNAME=Rafferty; DEPTID0=31; NAME=Sales",
            "DEPTID=33; LASTNAME=Jones; DEPTID0=33; NAME=Engineering",
            "DEPTID=33; LASTNAME=Steinberg; DEPTID0=33; NAME=Engineering",
            "DEPTID=34; LASTNAME=Robinson; DEPTID0=34; NAME=Clerical",
            "DEPTID=34; LASTNAME=Smith; DEPTID0=34; NAME=Clerical",
            "DEPTID=null; LASTNAME=John; DEPTID0=null; NAME=null",
            "DEPTID=null; LASTNAME=null; DEPTID0=35; NAME=Marketing").planContains("'type':'outer'");
  }

  /**
   * Join on subquery; also tests that if a field of the same name exists in both inputs, both fields make it through
   * the join.
   */
  @Test
  public void testJoinOnSubquery() throws Exception {
    JdbcAssert
        .withModel(MODEL, "HR")
        .sql(
            "select * from (\n" + "select deptId, lastname, 'x' as name from emp) as e\n"
                + " join dept on e.deptId = dept.deptId")
        .returnsUnordered("DEPTID=31; LASTNAME=Rafferty; NAME=x; DEPTID0=31; NAME0=Sales",
            "DEPTID=33; LASTNAME=Jones; NAME=x; DEPTID0=33; NAME0=Engineering",
            "DEPTID=33; LASTNAME=Steinberg; NAME=x; DEPTID0=33; NAME0=Engineering",
            "DEPTID=34; LASTNAME=Robinson; NAME=x; DEPTID0=34; NAME0=Clerical",
            "DEPTID=34; LASTNAME=Smith; NAME=x; DEPTID0=34; NAME0=Clerical").planContains("'type':'inner'");
  }

  /** Tests that one of the FoodMart tables is present. */
  @Test @Ignore
  public void testFoodMart() throws Exception {
    JdbcAssert
        .withModel(MODEL, "FOODMART")
        .sql("select * from product_class where cast(_map['product_class_id'] as integer) < 3")
        .returnsUnordered(
            "_MAP={product_category=Seafood, product_class_id=2, product_department=Seafood, product_family=Food, product_subcategory=Shellfish}",
            "_MAP={product_category=Specialty, product_class_id=1, product_department=Produce, product_family=Food, product_subcategory=Nuts}");
  }

  @Test
  public void testUnionAll() throws Exception {
    JdbcAssert.withModel(MODEL, "HR").sql("select deptId from dept\n" + "union all\n" + "select deptId from emp")
        .returnsUnordered("DEPTID=31", "DEPTID=33", "DEPTID=34", "DEPTID=35", "DEPTID=null")
        .planContains("'op':'union','distinct':false");
  }

  @Test
  public void testUnion() throws Exception {
    JdbcAssert.withModel(MODEL, "HR").sql("select deptId from dept\n" + "union\n" + "select deptId from emp")
        .returnsUnordered("DEPTID=31", "DEPTID=33", "DEPTID=34", "DEPTID=35", "DEPTID=null")
        .planContains("'op':'union','distinct':true");
  }

  @Test
  public void testOrderByDescNullsFirst() throws Exception {
    // desc nulls last
    JdbcAssert
        .withModel(MODEL, "HR")
        .sql("select * from emp order by deptId desc nulls first")
        .returns(
            "DEPTID=null; LASTNAME=John\n" + "DEPTID=34; LASTNAME=Robinson\n" + "DEPTID=34; LASTNAME=Smith\n"
                + "DEPTID=33; LASTNAME=Jones\n" + "DEPTID=33; LASTNAME=Steinberg\n" + "DEPTID=31; LASTNAME=Rafferty\n")
        .planContains("'op':'order'");
  }

  @Test
  public void testOrderByDescNullsLast() throws Exception {
    // desc nulls first
    JdbcAssert
        .withModel(MODEL, "HR")
        .sql("select * from emp order by deptId desc nulls last")
        .returns(
            "DEPTID=34; LASTNAME=Robinson\n" + "DEPTID=34; LASTNAME=Smith\n" + "DEPTID=33; LASTNAME=Jones\n"
                + "DEPTID=33; LASTNAME=Steinberg\n" + "DEPTID=31; LASTNAME=Rafferty\n" + "DEPTID=null; LASTNAME=John\n")
        .planContains("'op':'order'");
  }

  @Test @Ignore
  public void testOrderByDesc() throws Exception {
    // desc is implicitly "nulls first" (i.e. null sorted as +inf)
    // Current behavior is to sort nulls last. This is wrong.
    JdbcAssert
        .withModel(MODEL, "HR")
        .sql("select * from emp order by deptId desc")
        .returns(
            "DEPTID=null; LASTNAME=John\n" + "DEPTID=34; LASTNAME=Robinson\n" + "DEPTID=34; LASTNAME=Smith\n"
                + "DEPTID=33; LASTNAME=Jones\n" + "DEPTID=33; LASTNAME=Steinberg\n" + "DEPTID=31; LASTNAME=Rafferty\n")
        .planContains("'op':'order'");
  }

  @Test
  public void testOrderBy() throws Exception {
    // no sort order specified is implicitly "asc", and asc is "nulls last"
    JdbcAssert
        .withModel(MODEL, "HR")
        .sql("select * from emp order by deptId")
        .returns(
            "DEPTID=31; LASTNAME=Rafferty\n"
            + "DEPTID=33; LASTNAME=Jones\n"
            + "DEPTID=33; LASTNAME=Steinberg\n"
            + "DEPTID=34; LASTNAME=Robinson\n"
            + "DEPTID=34; LASTNAME=Smith\n"
            + "DEPTID=null; LASTNAME=John\n")
        .planContains("'op':'order'");
  }
}

// End JdbcTest.java
