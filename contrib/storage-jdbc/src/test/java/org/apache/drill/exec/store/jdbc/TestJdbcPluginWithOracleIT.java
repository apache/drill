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

import org.apache.drill.PlanTestBase;
import org.joda.time.DateTime;
import org.junit.Test;

/**
 * JDBC storage plugin tests against Oracle.
 */
public class TestJdbcPluginWithOracleIT extends PlanTestBase {

    @Test
    public void baseTest() throws Exception {
        testBuilder()
                .sqlQuery(
                        "select PERSON_ID, " +
                                "FIRST_NAME, LAST_NAME, ADDRESS, CITY, STATE_FIELD, ZIP, " +
                                "JSON_FIELD, SMALLINT_FIELD, NUMERIC_FIELD, BOOLEAN_FIELD, " +
                                "DOUBLE_FIELD, FLOAT_FIELD, REAL_FIELD, TIMESTAMP_FIELD, " +
                                "DATE_FIELD, TEXT_FIELD, NCHAR_FIELD, BLOB_FIELD, " +
                                "CLOB_FIELD, DECIMAL_FIELD, DEC_FIELD " +
                                "from oracle.`SYSTEM`.`person`")
                .ordered()
                .baselineColumns("PERSON_ID",
                        "FIRST_NAME", "LAST_NAME", "ADDRESS", "CITY", "STATE_FIELD", "ZIP",
                        "JSON_FIELD", "SMALLINT_FIELD", "NUMERIC_FIELD", "BOOLEAN_FIELD",
                        "DOUBLE_FIELD", "FLOAT_FIELD", "REAL_FIELD", "TIMESTAMP_FIELD",
                        "DATE_FIELD", "TEXT_FIELD", "NCHAR_FIELD", "BLOB_FIELD",
                        "CLOB_FIELD", "DECIMAL_FIELD", "DEC_FIELD")
                .baselineValues(1.0,
                        "first_name_1", "last_name_1", "1401 John F Kennedy Blvd", "Philadelphia", "PA", 19107.0,
                        "{ a : 5, b : 6 }", 10.0, 123.0, 0.0, 1.34, 1.112, 1.224, new DateTime(2011, 2, 11, 23, 12, 12),
                        new DateTime(2015, 5, 2, 0, 0),
                        "It is a long established fact that a reader will be distracted by the readable content of a page when looking at its layout",
                        "1", "this is test".getBytes(), "n----", 123.0, 1.0)
                .baselineValues(2.0,
                        "first_name_2", "last_name_2", "One Ferry Building", "San Francisco", "CA", 94111.0,
                        "{ name : Sem, surname : Sem }", 4.0, 45.0, 1.0, 2.54, 4.242, 4.252, new DateTime(2015, 12, 20, 23, 12, 12),
                        new DateTime(2015, 12, 1, 0, 0), "Some text", "1", "this is test2".getBytes(), "n!!!!",
                        14.0, 0.0)
                .baselineValues(3.0,
                        "first_name_3", "last_name_3", "176 Bowery", "New York", "NY", 10012.0,
                        "{ x : 12, y : 16 }", 0.0, 13.0, 0.0, 41.42, 2.421, 6.12, new DateTime(1901, 1, 1, 23, 12, 12),
                        new DateTime(2015, 5, 2, 12, 12, 30), "Some long text 3", "0", "this is test3".getBytes(), "n????",
                        0.0, 1.0)
                .baselineValues(5.0,
                        null, null, null, null, null, null,
                        null, null, null, null, null, null,
                        null, null, null, null, null, null,
                        null, null, null)

                .build().run();
    }

    @Test
    public void pushdownJoin() throws Exception {
        String query = "select x.person_id from (select person_id from oracle.`SYSTEM`.`person`) x "
                + "join (select person_id from oracle.`SYSTEM`.`person`) y on x.person_id = y.person_id ";
        testPlanMatchingPatterns(query, new String[]{ "JOIN" }, new String[]{ "Join" });
    }

    @Test
    public void pushdownJoinAndFilter() throws Exception {
        final String query = "select * from oracle.`SYSTEM`.`person` e " +
                "inner join oracle.`SYSTEM`.`person` s on e.first_name = s.first_name " +
                "where e.last_name <> 'hello'";

        testPlanMatchingPatterns(query, new String[] { "INNER JOIN" }, new String[] { "Join", "Filter" });
    }

    @Test
    public void pushdownDrillFunc() throws Exception {
        // Both subqueries use drill functions so join can not be pushed down to jdbc
        final String query = "select x.first_name from " +
                "(select CONVERT_TO(first_name, 'UTF8') first_name, person_id from oracle.`SYSTEM`.`person`) x join " +
                "(select CONVERT_TO(first_name, 'UTF8') first_name, person_id from oracle.`SYSTEM`.`person`) y on x.person_id = y.person_id";

        testPlanMatchingPatterns(query, new String[] { "Join" }, new String[] { "JOIN" });
    }

    @Test
    public void queryTest1() throws Exception {
        testBuilder()
                .sqlQuery(
                        "SELECT FIRST_NAME, LAST_NAME " +
                                "FROM oracle.`SYSTEM`.`EMPLOYEES` WHERE  FIRST_NAME  LIKE 'G%'")
                .ordered()
                .baselineColumns("FIRST_NAME", "LAST_NAME")
                .baselineValues("Guy","Himuro")
                .baselineValues("Gerald","Cambrault")
                .baselineValues("Girard","Geoni")
                .build()
                .run();
    }

    @Test
    public void queryTest2() throws Exception {
        testBuilder()
                .sqlQuery(
                        "SELECT DEPARTMENT_NAME, FIRST_NAME, CITY " +
                                "FROM oracle.`SYSTEM`.`DEPARTMENTS` D " +
                                "JOIN oracle.`SYSTEM`.`EMPLOYEES` E ON (D.MANAGER_ID=E.EMPLOYEE_ID) " +
                                "JOIN oracle.`SYSTEM`.`LOCATIONS` L USING (LOCATION_ID) WHERE FIRST_NAME LIKE 'A%'" )
                .ordered()
                .baselineColumns("DEPARTMENT_NAME", "FIRST_NAME", "CITY")
                .baselineValues("IT", "Alexander", "Southlake")
                .baselineValues("Shipping", "Adam", "South San Francisco")
                .build()
                .run();
    }

    @Test
    public void queryTest3() throws Exception {
        testBuilder()
                .sqlQuery(
                        "SELECT DEPARTMENT_ID, AVG(SALARY) AS AVGS " +
                                "FROM oracle.`SYSTEM`.`EMPLOYEES` WHERE COMMISSION_PCT IS NOT NULL GROUP BY DEPARTMENT_ID")
                .ordered()
                .baselineColumns("DEPARTMENT_ID", "AVGS")
                .baselineValues(null, 7000.0)
                .baselineValues(80.0, 8955.882352941177)
                .build()
                .run();
    }

    @Test
    public void queryTest4() throws Exception {
        testBuilder()
                .sqlQuery(
                        "SELECT DEPARTMENT_ID, TO_CHAR(HIRE_DATE,'YYYY') AS DATAS " +
                                "FROM oracle.`SYSTEM`.`EMPLOYEES` WHERE DEPARTMENT_ID <= 20 " +
                                "GROUP BY DEPARTMENT_ID, TO_CHAR(HIRE_DATE, 'YYYY') ORDER BY DEPARTMENT_ID")
                .ordered()
                .baselineColumns("DEPARTMENT_ID", "DATAS")
                .baselineValues(10.0, "1987")
                .baselineValues(20.0, "1996")
                .baselineValues(20.0, "1997")
                .build()
                .run();
    }

    @Test
    public void queryTest5() throws Exception {
        testBuilder()
                .sqlQuery(
                        "SELECT JOB_ID, COUNT(JOB_ID) AS COUNTS, SUM(SALARY) AS SUMS, MAX(SALARY)-MIN(SALARY) SALARY " +
                                "FROM oracle.`SYSTEM`.`EMPLOYEES` WHERE JOB_ID = 'AD_VP' GROUP BY JOB_ID")
                .ordered()
                .baselineColumns("JOB_ID","COUNTS", "SUMS", "SALARY")
                .baselineValues("AD_VP", 2.0, 34000.0, 0.0)
                .build()
                .run();
    }

    @Test
    public void queryTest6() throws Exception {
        testBuilder()
                .sqlQuery(
                        "SELECT * FROM oracle.`SYSTEM`.`DEPARTMENTS` " +
                                "WHERE DEPARTMENT_ID IN (SELECT DEPARTMENT_ID FROM oracle.`SYSTEM`.`EMPLOYEES` " +
                                "WHERE EMPLOYEE_ID IN (SELECT EMPLOYEE_ID FROM oracle.`SYSTEM`.`JOB_HISTORY`) " +
                                "GROUP BY DEPARTMENT_ID HAVING MAX(SALARY) >10000)")
                .ordered()
                .baselineColumns("DEPARTMENT_ID","DEPARTMENT_NAME", "MANAGER_ID", "LOCATION_ID")
                .baselineValues(30.0, "Purchasing", 114.0, 1700.0)
                .baselineValues(90.0, "Executive", 100.0, 1700.0)
                .baselineValues(20.0, "Marketing", 201.0, 1800.0)
                .build()
                .run();
    }

    @Test
    public void queryTest7() throws Exception {
        testBuilder()
                .sqlQuery(
                        "SELECT JH.* " +
                                "FROM oracle.`SYSTEM`.`JOB_HISTORY` JH " +
                                "JOIN oracle.`SYSTEM`.`EMPLOYEES` E ON (JH.EMPLOYEE_ID = E.EMPLOYEE_ID) WHERE SALARY > 15000"
                )
                .ordered()
                .baselineColumns("EMPLOYEE_ID", "START_DATE", "END_DATE", "JOB_ID", "DEPARTMENT_ID")
                .baselineValues(102.0, new DateTime(1993, 1, 13, 0, 0), new DateTime(1998, 7, 24, 0, 0), "AD_VP", 90.0)
                .baselineValues(101.0, new DateTime(1989, 9, 21, 0, 0), new DateTime(1993, 10, 27, 0, 0), "AD_VP", 90.0)
                .baselineValues(101.0, new DateTime(1993, 10, 28, 0, 0), new DateTime(1997, 3, 15, 0, 0), "AD_VP", 90.0)
                .build()
                .run();
    }

    @Test
    public void pushdownQueryTest1() throws Exception {
        String query = "select first_name, last_name "
                + "from oracle.`SYSTEM`.`EMPLOYEES` where first_name like 'G%' ";
        testPlanMatchingPatterns(query, new String[]{}, new String[]{"Like"});
    }

    @Test
    public void pushdownQueryTest2() throws Exception {
        String query = "select DEPARTMENT_NAME, FIRST_NAME, CITY "
                + "from oracle.`SYSTEM`.`DEPARTMENTS` D "
                + "join oracle.`SYSTEM`.`EMPLOYEES` E ON (D.MANAGER_ID=E.EMPLOYEE_ID) "
                + "join oracle.`SYSTEM`.`LOCATIONS` L USING (LOCATION_ID) where FIRST_NAME like 'A%' ";
        testPlanMatchingPatterns(query, new String[]{}, new String[]{"Join"});
    }

    @Test
    public void pushdownQueryTest3() throws Exception {
        String query = "select DEPARTMENT_ID, AVG(SALARY) as AVGS "
                + "from oracle.`SYSTEM`.`EMPLOYEES` where COMMISSION_PCT IS NOT NULL group by DEPARTMENT_ID";
        testPlanMatchingPatterns(query, new String[]{}, new String[]{"Aggregate", "Filter"});
    }

    @Test
    public void pushdownQueryTest4() throws Exception {
        String query = "select DEPARTMENT_ID, TO_CHAR(HIRE_DATE,'YYYY') as DATAS "
                + "from oracle.`SYSTEM`.`EMPLOYEES` where DEPARTMENT_ID <= 20 "
                + "group by DEPARTMENT_ID, TO_CHAR(HIRE_DATE, 'YYYY') order by DEPARTMENT_ID";
        testPlanMatchingPatterns(query, new String[]{}, new String[]{"Filter"});
    }

    @Test
    public void pushdownQueryTest5() throws Exception {
        String query = "select JOB_ID, COUNT(JOB_ID) as COUNTS, SUM(SALARY) as SUMS, MAX(SALARY)-MIN(SALARY) SALARY "
                + "from oracle.`SYSTEM`.`EMPLOYEES` where JOB_ID = 'AD_VP' group by JOB_ID ";
        testPlanMatchingPatterns(query, new String[]{}, new String[]{"Aggregate"});
    }

    @Test
    public void pushdownQueryTest6() throws Exception {
        String query = "select * from oracle.`SYSTEM`.`DEPARTMENTS` "
                + "where DEPARTMENT_ID IN (select DEPARTMENT_ID from oracle.`SYSTEM`.`EMPLOYEES` "
                + "where EMPLOYEE_ID IN (select EMPLOYEE_ID from oracle.`SYSTEM`.`JOB_HISTORY`) "
                + "group by DEPARTMENT_ID HAVING MAX(SALARY) >10000) ";
        testPlanMatchingPatterns(query, new String[]{}, new String[]{"Filter"});
    }

    @Test
    public void pushdownQueryTest7() throws Exception {
        String query = "select JH.* "
                + "from oracle.`SYSTEM`.`JOB_HISTORY` JH "
                + "join oracle.`SYSTEM`.`EMPLOYEES` E ON (JH.EMPLOYEE_ID = E.EMPLOYEE_ID) where SALARY > 15000";
        testPlanMatchingPatterns(query, new String[]{}, new String[]{"Join", "Filter"});
    }

}
