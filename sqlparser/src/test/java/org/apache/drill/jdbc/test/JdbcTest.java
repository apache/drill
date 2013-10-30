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
package org.apache.drill.jdbc.test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.logical.PlanProperties;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.logical.data.*;
import org.apache.drill.exec.ref.ReferenceInterpreter;
import org.apache.drill.exec.ref.rse.ClasspathRSE;
import org.apache.drill.exec.ref.rse.QueueRSE;
import org.apache.drill.jdbc.test.JdbcAssert.TestDataConnection;
import org.junit.Assert;
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
    LogicalPlan plan = JdbcAssert
        .withModel(MODEL, "DONUTS")
        .sql("select _MAP['ppu'] as ppu from donuts")
        .logicalPlan();

    PlanProperties planProperties = plan.getProperties();
    Assert.assertEquals("optiq", planProperties.generator.type);
    Assert.assertEquals("na", planProperties.generator.info);
    Assert.assertEquals(1, planProperties.version);
    Assert.assertEquals(PlanProperties.PlanType.APACHE_DRILL_LOGICAL, planProperties.type);
    Map<String, StorageEngineConfig> seConfigs = plan.getStorageEngines();
    StorageEngineConfig config = seConfigs.get("donuts-json");
    Assert.assertTrue(config != null && config instanceof ClasspathRSE.ClasspathRSEConfig);
    config = seConfigs.get("queue");
    Assert.assertTrue(config != null && config instanceof QueueRSE.QueueRSEConfig);
    Scan scan = findOnlyOperator(plan, Scan.class);
    Assert.assertEquals("donuts-json", scan.getStorageEngine());
    Assert.assertEquals("_MAP", scan.getOutputReference().getPath());
    Project project = findOnlyOperator(plan, Project.class);
    Assert.assertEquals(1, project.getSelections().length);
    Assert.assertEquals(Scan.class, project.getInput().getClass());
    Store store = findOnlyOperator(plan, Store.class);
    Assert.assertEquals("queue", store.getStorageEngine());
    Assert.assertEquals("output sink", store.getMemo());
    Assert.assertEquals(Project.class, store.getInput().getClass());
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

  private static <T extends LogicalOperator> Iterable<T> findOperator(LogicalPlan plan, final Class<T> operatorClazz) {
    return (Iterable<T>) Iterables.filter(plan.getSortedOperators(), new Predicate<LogicalOperator>() {
      @Override
      public boolean apply(LogicalOperator input) {
        return input.getClass().equals(operatorClazz);
      }
    });
  }

  private static <T extends LogicalOperator> T findOnlyOperator(LogicalPlan plan, final Class<T> operatorClazz) {
    return Iterables.getOnlyElement(findOperator(plan, operatorClazz));
  }

  @Test
  public void testProjectFilterSubqueryPlan() throws Exception {
    LogicalPlan plan = JdbcAssert
        .withModel(MODEL, "DONUTS")
        .sql(
            "select d['name'] as name, d['xx'] as xx from (\n" + " select _MAP['donuts'] as d from donuts)\n"
                + "where cast(d['ppu'] as double) > 0.6")
        .logicalPlan();
    PlanProperties planProperties = plan.getProperties();
    Assert.assertEquals("optiq", planProperties.generator.type);
    Assert.assertEquals("na", planProperties.generator.info);
    Assert.assertEquals(1, planProperties.version);
    Assert.assertEquals(PlanProperties.PlanType.APACHE_DRILL_LOGICAL, planProperties.type);
    Map<String, StorageEngineConfig> seConfigs = plan.getStorageEngines();
    StorageEngineConfig config = seConfigs.get("donuts-json");
    Assert.assertTrue(config != null && config instanceof ClasspathRSE.ClasspathRSEConfig);
    config = seConfigs.get("queue");
    Assert.assertTrue(config != null && config instanceof QueueRSE.QueueRSEConfig);
    Scan scan = findOnlyOperator(plan, Scan.class);
    Assert.assertEquals("donuts-json", scan.getStorageEngine());
    Assert.assertEquals("_MAP", scan.getOutputReference().getPath());
    Filter filter = findOnlyOperator(plan, Filter.class);
    Assert.assertTrue(filter.getInput() instanceof Scan);
    Project[] projects = Iterables.toArray(findOperator(plan, Project.class), Project.class);
    Assert.assertEquals(2, projects.length);
    Assert.assertEquals(1, projects[0].getSelections().length);
    Assert.assertEquals(Filter.class, projects[0].getInput().getClass());
    Assert.assertEquals(2, projects[1].getSelections().length);
    Assert.assertEquals(Project.class, projects[1].getInput().getClass());
    Store store = findOnlyOperator(plan, Store.class);
    Assert.assertEquals("queue", store.getStorageEngine());
    Assert.assertEquals("output sink", store.getMemo());
    Assert.assertEquals(Project.class, store.getInput().getClass());
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

  
  @Ignore
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
        .planContains(CollapsingAggregate.class);
  }

  @Test
  public void testCountNoGroupBy() throws Exception {
    // 5 out of 6 employees have a not-null deptId
    JdbcAssert.withModel(MODEL, "HR").sql("select count(deptId) as cd, count(*) as c from emp").returns("CD=5; C=6\n")
        .planContains(CollapsingAggregate.class);
  }

  @Test
  public void testDistinctCountNoGroupBy() throws Exception {
    JdbcAssert.withModel(MODEL, "HR").sql("select count(distinct deptId) as c from emp").returns("C=3\n")
        .planContains(CollapsingAggregate.class);
  }

  @Test
  public void testDistinctCountGroupByEmpty() throws Exception {
    JdbcAssert.withModel(MODEL, "HR").sql("select count(distinct deptId) as c from emp group by ()").returns("C=3\n")
        .planContains(CollapsingAggregate.class);
  }

  @Test
  public void testCountNull() throws Exception {
    JdbcAssert.withModel(MODEL, "HR").sql("select count(distinct deptId) as c from emp group by ()").returns("C=3\n")
        .planContains(CollapsingAggregate.class);
  }

  @Test
  public void testCount() throws Exception {
    JdbcAssert.withModel(MODEL, "HR").sql("select deptId, count(*) as c from emp group by deptId")
        .returnsUnordered("DEPTID=31; C=1", "DEPTID=33; C=2", "DEPTID=34; C=2", "DEPTID=null; C=1")
        .planContains(CollapsingAggregate.class); // make sure using drill
  }

  @Test
  public void testJoin() throws Exception {
    Join join = JdbcAssert
        .withModel(MODEL, "HR")
        .sql("select * from emp join dept on emp.deptId = dept.deptId")
        .returnsUnordered("DEPTID=31; LASTNAME=Rafferty; DEPTID0=31; NAME=Sales",
            "DEPTID=33; LASTNAME=Jones; DEPTID0=33; NAME=Engineering",
            "DEPTID=33; LASTNAME=Steinberg; DEPTID0=33; NAME=Engineering",
            "DEPTID=34; LASTNAME=Robinson; DEPTID0=34; NAME=Clerical",
            "DEPTID=34; LASTNAME=Smith; DEPTID0=34; NAME=Clerical").planContains(Join.class);
    Assert.assertEquals(Join.JoinType.INNER, join.getJointType());
  }

  @Test
  public void testLeftJoin() throws Exception {
    Join join = JdbcAssert
        .withModel(MODEL, "HR")
        .sql("select * from emp left join dept on emp.deptId = dept.deptId")
        .returnsUnordered("DEPTID=31; LASTNAME=Rafferty; DEPTID0=31; NAME=Sales",
            "DEPTID=33; LASTNAME=Jones; DEPTID0=33; NAME=Engineering",
            "DEPTID=33; LASTNAME=Steinberg; DEPTID0=33; NAME=Engineering",
            "DEPTID=34; LASTNAME=Robinson; DEPTID0=34; NAME=Clerical",
            "DEPTID=34; LASTNAME=Smith; DEPTID0=34; NAME=Clerical",
            "DEPTID=null; LASTNAME=John; DEPTID0=null; NAME=null").planContains(Join.class);
    Assert.assertEquals(Join.JoinType.LEFT, join.getJointType());
  }

  /**
   * Right join is tricky because Drill's "join" operator only supports "left", so we have to flip inputs.
   */
  @Test @Ignore
  public void testRightJoin() throws Exception {
    Join join = JdbcAssert.withModel(MODEL, "HR").sql("select * from emp right join dept on emp.deptId = dept.deptId")
        .returnsUnordered("xx").planContains(Join.class);
    Assert.assertEquals(Join.JoinType.LEFT, join.getJointType());
  }

  @Test
  public void testFullJoin() throws Exception {
    Join join = JdbcAssert
        .withModel(MODEL, "HR")
        .sql("select * from emp full join dept on emp.deptId = dept.deptId")
        .returnsUnordered("DEPTID=31; LASTNAME=Rafferty; DEPTID0=31; NAME=Sales",
            "DEPTID=33; LASTNAME=Jones; DEPTID0=33; NAME=Engineering",
            "DEPTID=33; LASTNAME=Steinberg; DEPTID0=33; NAME=Engineering",
            "DEPTID=34; LASTNAME=Robinson; DEPTID0=34; NAME=Clerical",
            "DEPTID=34; LASTNAME=Smith; DEPTID0=34; NAME=Clerical",
            "DEPTID=null; LASTNAME=John; DEPTID0=null; NAME=null",
            "DEPTID=null; LASTNAME=null; DEPTID0=35; NAME=Marketing").planContains(Join.class);
    Assert.assertEquals(Join.JoinType.OUTER, join.getJointType());
  }

  /**
   * Join on subquery; also tests that if a field of the same name exists in both inputs, both fields make it through
   * the join.
   */
  @Test
  public void testJoinOnSubquery() throws Exception {
    Join join = JdbcAssert
        .withModel(MODEL, "HR")
        .sql(
            "select * from (\n" + "select deptId, lastname, 'x' as name from emp) as e\n"
                + " join dept on e.deptId = dept.deptId")
        .returnsUnordered("DEPTID=31; LASTNAME=Rafferty; NAME=x; DEPTID0=31; NAME0=Sales",
            "DEPTID=33; LASTNAME=Jones; NAME=x; DEPTID0=33; NAME0=Engineering",
            "DEPTID=33; LASTNAME=Steinberg; NAME=x; DEPTID0=33; NAME0=Engineering",
            "DEPTID=34; LASTNAME=Robinson; NAME=x; DEPTID0=34; NAME0=Clerical",
            "DEPTID=34; LASTNAME=Smith; NAME=x; DEPTID0=34; NAME0=Clerical").planContains(Join.class);
    Assert.assertEquals(Join.JoinType.INNER, join.getJointType());
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
    Union union = JdbcAssert.withModel(MODEL, "HR").sql("select deptId from dept\n" + "union all\n" + "select deptId from emp")
        .returnsUnordered("DEPTID=31", "DEPTID=33", "DEPTID=34", "DEPTID=35", "DEPTID=null")
        .planContains(Union.class);
    Assert.assertFalse(union.isDistinct());
  }

  @Test
  public void testUnion() throws Exception {
    Union union = JdbcAssert.withModel(MODEL, "HR").sql("select deptId from dept\n" + "union\n" + "select deptId from emp")
        .returnsUnordered("DEPTID=31", "DEPTID=33", "DEPTID=34", "DEPTID=35", "DEPTID=null")
        .planContains(Union.class);
    Assert.assertTrue(union.isDistinct());
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
        .planContains(Order.class);
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
        .planContains(Order.class);
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
        .planContains(Order.class);
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
        .planContains(Order.class);
  }

  @Test
  public void testLimit() throws Exception {
    JdbcAssert
        .withModel(MODEL, "HR")
        .sql("select LASTNAME from emp limit 2")
        .returns("LASTNAME=Rafferty\n" +
            "LASTNAME=Jones")
        .planContains(Limit.class);
  }

  
  @Test
  public void testLimitOrderBy() throws Exception {
    TestDataConnection tdc = JdbcAssert
        .withModel(MODEL, "HR")
        .sql("select LASTNAME from emp order by LASTNAME limit 2")
        .returns("LASTNAME=John\n" +
            "LASTNAME=Jones");
        tdc.planContains(Limit.class);
        tdc.planContains(Order.class);
        
  }
  
  @Test
  public void testOrderByWithOffset() throws Exception {
    JdbcAssert
        .withModel(MODEL, "HR")
        .sql("select LASTNAME from emp order by LASTNAME asc offset 3")
        .returns("LASTNAME=Robinson\n" +
            "LASTNAME=Smith\n" +
            "LASTNAME=Steinberg")
        .planContains(Limit.class);

  }

  @Test
  public void testOrderByWithOffsetAndFetch() throws Exception {
    JdbcAssert
        .withModel(MODEL, "HR")
        .sql("select LASTNAME from emp order by LASTNAME asc offset 3 fetch next 2 rows only")
        .returns("LASTNAME=Robinson\n" +
            "LASTNAME=Smith")
        .planContains(Limit.class);
  }
}

// End JdbcTest.java
