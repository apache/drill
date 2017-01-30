/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to you under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.drill.exec.fn.impl.testing;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.exceptions.UserException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;
public class TestDateConversions extends BaseTestQuery {

  private static String TEMP_DIR;

  @BeforeClass
  public static void generateTestFiles() throws IOException {
    File path = new File(BaseTestQuery.getTempDir("json/input"));
    path.mkdirs();
    TEMP_DIR = path.toPath().toString();

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(path, "joda_postgres_date.json")))) {
      writer.write("{\"date1\" : \"1970-01-02\",\n \"date2\" : \"01021970\",\n \"date3\" : \"32/1970\"\n}\n"
        + "{\"date1\" : \"2010-05-03\",\n \"date2\" : \"01021970\",\n \"date3\" : \"64/2010\"\n}");
    }

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(path, "joda_postgres_time.json")))) {
      writer.write("{\"time1\" : \"23:11:59\",\n \"time2\" : \"11:11:59pm\",\n \"time3\" : \"591111pm\"\n}\n"
        + "{\"time1\" : \"17:33:41\",\n \"time2\" : \"5:33:41am\",\n \"time3\" : \"413305pm\"\n}");
    }

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(path, "joda_postgres_date_time.json")))) {
      writer.write("{ \"time1\" : \"1970-01-0223:11:59\",\n \"time2\" : \"0102197011:11:59pm\",\n"
        + "  \"time3\" : \"32/1970591111pm\"\n}\n"
        + "{\"time1\" : \"2010-05-0317:33:41\",\n \"time2\" : \"0102197005:33:41am\",\n"
        + "  \"time3\" : \"64/2010413305pm\"\n}");
    }
  }

  @AfterClass
  public static void deleteTestFiles() throws IOException {
    java.nio.file.Files.delete(new File(TEMP_DIR, "joda_postgres_date.json").toPath());
    java.nio.file.Files.delete(new File(TEMP_DIR, "joda_postgres_time.json").toPath());
    java.nio.file.Files.delete(new File(TEMP_DIR, "joda_postgres_date_time.json").toPath());
  }

  @Test
  public void testJodaDate() throws Exception {
    String query = String.format("SELECT to_date(date1, 'yyyy-dd-MM') = "
      + "to_date(date2, 'ddMMyyyy') as col1, " + "to_date(date1, 'yyyy-dd-MM') = "
      + "to_date(date3, 'D/yyyy') as col2 "
      + "from dfs_test.`%s/joda_postgres_date.json`", TEMP_DIR);

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("col1", "col2")
      .baselineValues(true, true)
      .baselineValues(false, true)
      .go();
  }

  @Test
  public void testPostgresDate() throws Exception {
    String query = String.format("SELECT sql_to_date(date1, 'yyyy-DD-MM') = "
      + "sql_to_date(date2, 'DDMMyyyy') as col1, "
      + "sql_to_date(date1, 'yyyy-DD-MM') = "
      + "sql_to_date(date3, 'DDD/yyyy') as col2 "
      + "from dfs_test.`%s/joda_postgres_date.json`", TEMP_DIR);

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("col1", "col2")
      .baselineValues(true, true)
      .baselineValues(false, true)
      .go();
  }

  @Test
  public void testJodaTime() throws Exception {
    String query = String.format("SELECT to_time(time1, 'H:m:ss') = "
      + "to_time(time2, 'h:m:ssa') as col1, "
      + "to_time(time1, 'H:m:ss') = "
      + "to_time(time3, 'ssmha') as col2 "
      + "from dfs_test.`%s/joda_postgres_time.json`", TEMP_DIR);

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("col1", "col2")
      .baselineValues(true, true)
      .baselineValues(false, true)
      .go();
  }

  @Test
  public void testPostgresTime() throws Exception {
    String query = String.format("SELECT sql_to_time(time1, 'HH24:MI:SS') = "
      + "sql_to_time(time2, 'HH12:MI:SSam') as col1, "
      + "sql_to_time(time1, 'HH24:MI:SS') = "
      + "sql_to_time(time3, 'SSMIHH12am') as col2 "
      + "from dfs_test.`%s/joda_postgres_time.json`", TEMP_DIR);

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("col1", "col2")
      .baselineValues(true, true)
      .baselineValues(false, true)
      .go();
  }

  @Test
  public void testPostgresDateTime() throws Exception {
    String query = String.format("SELECT sql_to_timestamp(time1, 'yyyy-DD-MMHH24:MI:SS') = "
      + "sql_to_timestamp(time2, 'DDMMyyyyHH12:MI:SSam') as col1, "
      + "sql_to_timestamp(time1, 'yyyy-DD-MMHH24:MI:SS') = "
      + "sql_to_timestamp(time3, 'DDD/yyyySSMIHH12am') as col2 "
      + "from dfs_test.`%s/joda_postgres_date_time.json`", TEMP_DIR);

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("col1", "col2")
      .baselineValues(true, true)
      .baselineValues(false, true)
      .go();

  }

  @Test
  public void testJodaDateTime() throws Exception {
    String query = String.format("SELECT to_timestamp(time1, 'yyyy-dd-MMH:m:ss') = "
      + "to_timestamp(time2, 'ddMMyyyyh:m:ssa') as col1, "
      + "to_timestamp(time1, 'yyyy-dd-MMH:m:ss') = "
      + "to_timestamp(time3, 'DDD/yyyyssmha') as col2 "
      + "from dfs_test.`%s/joda_postgres_date_time.json`", TEMP_DIR);

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("col1", "col2")
      .baselineValues(true, true)
      .baselineValues(false, true)
      .go();
  }

  @Test
  public void testJodaDateTimeNested() throws Exception {
    String query = String.format("SELECT date_add(to_date(time1, concat('yyyy-dd-MM','H:m:ss')), 22)= "
      + "date_add(to_date(time2, concat('ddMMyyyy', 'h:m:ssa')), 22) as col1, "
      + "date_add(to_date(time1, concat('yyyy-dd-MM', 'H:m:ss')), 22) = "
      + "date_add(to_date(time3, concat('DDD/yyyy', 'ssmha')), 22) as col2 "
      + "from dfs_test.`%s/joda_postgres_date_time.json`", TEMP_DIR);

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("col1", "col2")
      .baselineValues(true, true)
      .baselineValues(false, true)
      .go();

  }

  @Test
  public void testPostgresDateTimeNested() throws Exception {
    String query = String.format("SELECT date_add(sql_to_date(time1, concat('yyyy-DD-MM', 'HH24:MI:SS')), 22) = "
      + "date_add(sql_to_date(time2, concat('DDMMyyyy', 'HH12:MI:SSam')), 22) as col1, "
      + "date_add(sql_to_date(time1, concat('yyyy-DD-MM', 'HH24:MI:SS')), 10) = "
      + "date_add(sql_to_date(time3, concat('DDD/yyyySSMI', 'HH12am')), 10) as col2 "
      + "from dfs_test.`%s/joda_postgres_date_time.json`", TEMP_DIR);

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("col1", "col2")
      .baselineValues(true, true)
      .baselineValues(false, true)
      .go();
  }

  @Test(expected = UserException.class)
  public void testPostgresPatternFormatError() throws Exception {
    try {
      test("SELECT sql_to_date('1970-01-02', 'yyyy-QQ-MM') from (values(1))");
    } catch (UserException e) {
      assertThat("No expected current \"FUNCTION ERROR\"", e.getMessage(), startsWith("FUNCTION ERROR"));
      throw e;
    }
  }

  @Test(expected = UserException.class)
  public void testPostgresDateFormatError() throws Exception {
    try {
      test("SELECT sql_to_date('1970/01/02', 'yyyy-DD-MM') from (values(1))");
    } catch (UserException e) {
      assertThat("No expected current \"FUNCTION ERROR\"", e.getMessage(), startsWith("FUNCTION ERROR"));
      throw e;
    }
  }
}
