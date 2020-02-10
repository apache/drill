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
package org.apache.drill.exec.store.jdbc;

import com.wix.mysql.EmbeddedMysql;
import com.wix.mysql.ScriptResolver;
import com.wix.mysql.config.MysqldConfig;
import com.wix.mysql.config.SchemaConfig;
import com.wix.mysql.distribution.Version;
import org.apache.drill.categories.JdbcStorageTest;
import org.apache.drill.exec.expr.fn.impl.DateUtility;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryTestUtil;
import org.joda.time.DateTimeZone;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

/**
 * JDBC storage plugin tests against MySQL.
 * Note: it requires libaio.so library in the system
 */
@Category(JdbcStorageTest.class)
public class TestJdbcPluginWithMySQLIT extends ClusterTest {

  private static EmbeddedMysql mysqld;

  @BeforeClass
  public static void initMysql() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    String mysqlDBName = "drill_mysql_test";
    int mysqlPort = QueryTestUtil.getFreePortNumber(2215, 300);

    MysqldConfig config = MysqldConfig.aMysqldConfig(Version.v5_6_21)
        .withPort(mysqlPort)
        .withUser("mysqlUser", "mysqlPass")
        .withTimeZone(DateTimeZone.UTC.toTimeZone())
        .build();

    SchemaConfig.Builder schemaConfig = SchemaConfig.aSchemaConfig(mysqlDBName)
        .withScripts(ScriptResolver.classPathScript("mysql-test-data.sql"));

    String osName = System.getProperty("os.name").toLowerCase();
    if (osName.startsWith("linux")) {
      schemaConfig.withScripts(ScriptResolver.classPathScript("mysql-test-data-linux.sql"));
    }

    mysqld = EmbeddedMysql.anEmbeddedMysql(config).addSchema(schemaConfig.build()).start();

    JdbcStorageConfig jdbcStorageConfig = new JdbcStorageConfig("com.mysql.cj.jdbc.Driver",
        String.format("jdbc:mysql://localhost:%s/%s?useJDBCCompliantTimezoneShift=true", mysqlPort, mysqlDBName),
        "mysqlUser", "mysqlPass", false, null);
    jdbcStorageConfig.setEnabled(true);

    cluster.defineStoragePlugin("mysql", jdbcStorageConfig);

    if (osName.startsWith("linux")) {
      // adds storage plugin with case insensitive table names
      JdbcStorageConfig jdbcCaseSensitiveStorageConfig = new JdbcStorageConfig("com.mysql.cj.jdbc.Driver",
          String.format("jdbc:mysql://localhost:%s/%s?useJDBCCompliantTimezoneShift=true", mysqlPort, mysqlDBName),
          "mysqlUser", "mysqlPass", true, null);
      jdbcCaseSensitiveStorageConfig.setEnabled(true);
      cluster.defineStoragePlugin("mysqlCaseInsensitive", jdbcCaseSensitiveStorageConfig);
    }
  }

  @AfterClass
  public static void stopMysql() {
    if (mysqld != null) {
      mysqld.stop();
    }
  }

  @Test
  public void validateResult() throws Exception {

    testBuilder()
        .sqlQuery(
            "select person_id, " +
                "first_name, last_name, address, city, state, zip, " +
                "bigint_field, smallint_field, numeric_field, " +
                "boolean_field, double_field, float_field, real_field, " +
                "date_field, datetime_field, year_field, time_field, " +
                "json, text_field, tiny_text_field, medium_text_field, long_text_field, " +
                "blob_field, bit_field, enum_field " +
            "from mysql.`drill_mysql_test`.person")
        .ordered()
        .baselineColumns("person_id",
            "first_name", "last_name", "address", "city", "state", "zip",
            "bigint_field", "smallint_field", "numeric_field",
            "boolean_field",
            "double_field", "float_field", "real_field",
            "date_field", "datetime_field", "year_field", "time_field",
            "json", "text_field", "tiny_text_field", "medium_text_field", "long_text_field",
            "blob_field", "bit_field", "enum_field")
        .baselineValues(1,
            "first_name_1", "last_name_1", "1401 John F Kennedy Blvd", "Philadelphia", "PA", 19107,
            123456789L, 1, new BigDecimal("10.01"),
            false,
            1.0, 1.1, 1.2,
            DateUtility.parseLocalDate("2012-02-29"), DateUtility.parseLocalDateTime("2012-02-29 13:00:01.0"), DateUtility.parseLocalDate("2015-01-01"), DateUtility.parseLocalTime("13:00:01.0"),
            "{ a : 5, b : 6 }",
            "It is a long established fact that a reader will be distracted by the readable content of a page when looking at its layout",
            "xxx",
            "a medium piece of text",
            "a longer piece of text this is going on.....",
            "this is a test".getBytes(),
            true, "XXX")
        .baselineValues(2,
            "first_name_2", "last_name_2", "One Ferry Building", "San Francisco", "CA", 94111,
            45456767L, 3, new BigDecimal("30.04"),
            true,
            3.0, 3.1, 3.2,
            DateUtility.parseLocalDate("2011-10-30"), DateUtility.parseLocalDateTime("2011-10-30 11:34:21.0"), DateUtility.parseLocalDate("2015-01-01"), DateUtility.parseLocalTime("11:34:21.0"),
            "{ z : [ 1, 2, 3 ] }",
            "It is a long established fact that a reader will be distracted by the readable content of a page when looking at its layout",
            "abc",
            "a medium piece of text 2",
            "somewhat more text",
            "this is a test 2".getBytes(),
            false, "YYY")
        .baselineValues(3,
            "first_name_3", "last_name_3", "176 Bowery", "New York", "NY", 10012,
            123090L, -3, new BigDecimal("55.12"),
            false,
            5.0, 5.1, 5.55,
            DateUtility.parseLocalDate("2015-06-01"), DateUtility.parseLocalDateTime("2015-09-22 15:46:10.0"), DateUtility.parseLocalDate("1901-01-01"), DateUtility.parseLocalTime("16:00:01.0"),
            "{ [ a, b, c ] }",
            "Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit",
            "abc",
            "a medium piece of text 3",
            "somewhat more text",
            "this is a test 3".getBytes(),
            true, "ZZZ")
        .baselineValues(5,
            null, null, null, null, null, null,
            null, null, null,
            null,
            null, null, null,
            null, null, null, null,
            null,
            null,
            null,
            null,
            null,
            null,
            null, "XXX")
        .go();
  }

  @Test
  public void pushDownJoin() throws Exception {
    String query = "select x.person_id from (select person_id from mysql.`drill_mysql_test`.person) x "
            + "join (select person_id from mysql.`drill_mysql_test`.person) y on x.person_id = y.person_id";
    queryBuilder()
        .sql(query)
        .planMatcher()
        .exclude("Join")
        .match();
  }

  @Test
  public void pushDownJoinAndFilterPushDown() throws Exception {
    String query = "select * from " +
            "mysql.`drill_mysql_test`.person e " +
            "INNER JOIN " +
            "mysql.`drill_mysql_test`.person s " +
            "ON e.first_name = s.first_name " +
            "WHERE e.last_name > 'hello'";

    queryBuilder()
        .sql(query)
        .planMatcher()
        .exclude("Join", "Filter")
        .match();
  }

  @Test
  public void testPhysicalPlanSubmission() throws Exception {
    String query = "select * from mysql.`drill_mysql_test`.person";
    String plan = queryBuilder().sql(query).explainJson();
    assertEquals(4, queryBuilder().physical(plan).run().recordCount());
  }

  @Test
  public void emptyOutput() {
    String query = "select * from mysql.`drill_mysql_test`.person e limit 0";

    testBuilder()
        .sqlQuery(query)
        .expectsEmptyResultSet();
  }

  @Test
  public void testCaseSensitiveTableNames() throws Exception {
    String osName = System.getProperty("os.name").toLowerCase();
    Assume.assumeTrue(
        "Skip tests for non-linux systems due to " +
            "table names case-insensitivity problems on Windows and MacOS",
        osName.startsWith("linux"));
    run("use mysqlCaseInsensitive.`drill_mysql_test`");
    // two table names match the filter ignoring the case
    assertEquals(2, queryBuilder().sql("show tables like 'caseSensitiveTable'").run().recordCount());

    run("use mysql.`drill_mysql_test`");
    // single table matches the filter considering table name the case
    assertEquals(1, queryBuilder().sql("show tables like 'caseSensitiveTable'").run().recordCount());

    // checks that tables with names in different case are recognized correctly
    assertEquals(1, queryBuilder().sql("describe caseSensitiveTable").run().recordCount());
    assertEquals(2, queryBuilder().sql("describe CASESENSITIVETABLE").run().recordCount());
  }

  @Test // DRILL-6734
  public void testExpressionsWithoutAlias() throws Exception {
    String query = "select count(*), 1+1+2+3+5+8+13+21+34, (1+sqrt(5))/2\n" +
        "from mysql.`drill_mysql_test`.person";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("EXPR$0", "EXPR$1", "EXPR$2")
        .baselineValues(4L, 88L, 1.618033988749895)
        .go();
  }

  @Test // DRILL-6734
  public void testExpressionsWithoutAliasesPermutations() throws Exception {
    String query = "select EXPR$1, EXPR$0, EXPR$2\n" +
        "from (select 1+1+2+3+5+8+13+21+34, (1+sqrt(5))/2, count(*) from mysql.`drill_mysql_test`.person)";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("EXPR$1", "EXPR$0", "EXPR$2")
        .baselineValues(1.618033988749895, 88L, 4L)
        .go();
  }

  @Test // DRILL-6734
  public void testExpressionsWithAliases() throws Exception {
    String query = "select person_id as ID, 1+1+2+3+5+8+13+21+34 as FIBONACCI_SUM, (1+sqrt(5))/2 as golden_ratio\n" +
        "from mysql.`drill_mysql_test`.person limit 2";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("ID", "FIBONACCI_SUM", "golden_ratio")
        .baselineValues(1, 88L, 1.618033988749895)
        .baselineValues(2, 88L, 1.618033988749895)
        .go();
  }

  @Test // DRILL-6893
  public void testJoinStar() throws Exception {
    String query = "select * from (select person_id from mysql.`drill_mysql_test`.person) t1 join " +
        "(select person_id from mysql.`drill_mysql_test`.person) t2 on t1.person_id = t2.person_id";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("person_id", "person_id0")
        .baselineValues(1, 1)
        .baselineValues(2, 2)
        .baselineValues(3, 3)
        .baselineValues(5, 5)
        .go();
  }

  @Test
  public void testSemiJoin() throws Exception {
    String query =
        "select person_id from mysql.`drill_mysql_test`.person t1\n" +
            "where exists (" +
                "select person_id from mysql.`drill_mysql_test`.person\n" +
                "where t1.person_id = person_id)";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("person_id")
        .baselineValuesForSingleColumn(1, 2, 3, 5)
        .go();
  }

  @Test
  public void testInformationSchemaViews() throws Exception {
    String query = "select * from information_schema.`views`";
    run(query);
  }

  @Test
  public void testJdbcTableTypes() throws Exception {
    String query = "select distinct table_type from information_schema.`tables` " +
        "where table_schema like 'mysql%'";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("table_type")
        .baselineValuesForSingleColumn("SYSTEM VIEW", "TABLE", "VIEW")
        .go();
  }

  @Test
  public void testLimitPushDown() throws Exception {
    String query = "select person_id from mysql.`drill_mysql_test`.person limit 10";
    queryBuilder()
        .sql(query)
        .planMatcher()
        .include("Jdbc\\(.*LIMIT 10")
        .exclude("Limit\\(")
        .match();
  }
}
