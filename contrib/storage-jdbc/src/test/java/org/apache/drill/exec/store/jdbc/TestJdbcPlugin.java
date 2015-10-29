package org.apache.drill.exec.store.jdbc;

import static org.junit.Assert.assertEquals;

import java.net.InetAddress;
import java.sql.Connection;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.PlanTestBase;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
public class TestJdbcPlugin extends PlanTestBase {

  static NetworkServerControl server;

  @BeforeClass
  public static void setupDefaultTestCluster() throws Exception {
    System.setProperty("derby.drda.startNetworkServer", "true");
    server = new NetworkServerControl(InetAddress.getByName("localhost"),
        20000,
        "admin",
        "admin");
    java.io.PrintWriter consoleWriter = new java.io.PrintWriter(System.out, true);
    server.start(consoleWriter);

    BasicDataSource source = new BasicDataSource();
    source.setUrl("jdbc:derby://localhost:20000/memory:testDB;create=true");
    source.setDriverClassName("org.apache.derby.jdbc.ClientDriver");

    final String insertValues1 = "INSERT INTO person VALUES (1, 'Smith', null, '{number:\"123 Main\"}','mtrx', "
        + "'xy', 333.333, 444.444, 555.00, TIME('15:09:02'), DATE('1994-02-23'), TIMESTAMP('1962-09-23 03:23:34.234'),"
        + " 666.66, 1, -1, false)";
    final String insertValues2 = "INSERT INTO person (PersonId) VALUES (null)";
    try (Connection c = source.getConnection()) {
      c.createStatement().execute("CREATE TABLE person\n" +
          "(\n" +
          "PersonID int,\n" +
          "LastName varchar(255),\n" +
          "FirstName varchar(255),\n" +
          "Address varchar(255),\n" +
          "City varchar(255),\n" +
          "Code char(2),\n" +
          "dbl double,\n" +
          "flt float,\n" +
          "rel real,\n" +
          "tm time,\n" +
          "dt date,\n" +
          "tms timestamp,\n" +
          "num numeric(10,2), \n" +
          "sm smallint,\n" +
          "bi bigint,\n" +
          "bool boolean\n" +

          ")");

      c.createStatement().execute(insertValues1);
      c.createStatement().execute(insertValues2);
      c.createStatement().execute(insertValues1);
    }

    BaseTestQuery.setupDefaultTestCluster();
  }

  @AfterClass
  public static void shutdownDb() throws Exception {
    server.shutdown();
  }

  @Test
  public void validateResult() throws Exception {
    // we'll test data except for date, time and timestamps. Derby mangles these due to improper timezone support.
    testBuilder()
        .sqlQuery(
            "select PERSONID, LASTNAME, FIRSTNAME, ADDRESS, CITY, CODE, DBL, FLT, REL, NUM, SM, BI, BOOL from testdb.PERSON")
        .ordered()
        .baselineColumns("PERSONID", "LASTNAME", "FIRSTNAME", "ADDRESS", "CITY", "CODE", "DBL", "FLT", "REL",
            "NUM", "SM", "BI", "BOOL")
        .baselineValues(1, "Smith", null, "{number:\"123 Main\"}", "mtrx", "xy", 333.333, 444.444, 555.00,
            666.66, 1, -1l, false)
        .baselineValues(null, null, null, null, null, null, null, null, null, null, null, null, null)
        .baselineValues(1, "Smith", null, "{number:\"123 Main\"}", "mtrx", "xy", 333.333, 444.444, 555.00,
            666.66, 1, -1l, false)
        .build().run();
  }

  @Test
  public void queryDefaultSchema() throws Exception {
    testNoResult("select * from testdb.PERSON");
  }

  @Test
  public void queryDifferentCase() throws Exception {
    testNoResult("select * from testdb.person");
  }

  @Test
  public void pushdownJoin() throws Exception {
    testNoResult("use testdb");
    String query = "select x.PersonId from (select PersonId from person)x "
        + "join (select PersonId from person)y on x.PersonId = y.PersonId ";
    testPlanMatchingPatterns(query, new String[] {}, new String[] { "Join" });

  }

  @Test
  public void pushdownJoinAndFilterPushDown() throws Exception {
    final String query = "select * from \n" +
        "testdb.PERSON e\n" +
        "INNER JOIN \n" +
        "testdb.PERSON s\n" +
        "ON e.FirstName = s.FirstName\n" +
        "WHERE e.LastName > 'hello'";

    testPlanMatchingPatterns(query, new String[] {}, new String[] { "Join", "Filter" });
  }

  @Test
  public void pushdownAggregation() throws Exception {
    final String query = "select count(*) from \n" +
        "testdb.PERSON";

    testPlanMatchingPatterns(query, new String[] {}, new String[] { "Aggregate" });
  }

  @Test
  public void pushdownDoubleJoinAndFilter() throws Exception {
    final String query = "select * from \n" +
        "testdb.PERSON e\n" +
        "INNER JOIN \n" +
        "testdb.PERSON s\n" +
        "ON e.PersonId = s.PersonId\n" +
        "INNER JOIN \n" +
        "testdb.PERSON ed\n" +
        "ON e.PersonId = ed.PersonId\n" +
        "WHERE s.FirstName > 'abc' and ed.FirstName > 'efg'";
    testPlanMatchingPatterns(query, new String[] {}, new String[] { "Join", "Filter" });
  }

  @Test
  public void showTablesDefaultSchema() throws Exception {
    testNoResult("use testdb");
    assertEquals(1, testRunAndPrint(QueryType.SQL, "show tables like 'PERSON'"));
  }

  @Test
  public void describe() throws Exception {
    testNoResult("use testdb");
    assertEquals(16, testRunAndPrint(QueryType.SQL, "describe PERSON"));
  }

  @Test
  public void ensureDrillFunctionsAreNotPushedDown() throws Exception {
    // This should verify that we're not trying to push CONVERT_FROM into the JDBC storage plugin. If were pushing
    // this function down, the SQL query would fail.
    testNoResult("select CONVERT_FROM(Address, 'JSON') from testdb.person where PersonId = 1");
  }

  @Test
  public void pushdownFilter() throws Exception {
    testNoResult("use testdb");
    String query = "select * from person where PersonId = 1";
    testPlanMatchingPatterns(query, new String[] {}, new String[] { "Filter" });
  }
}
