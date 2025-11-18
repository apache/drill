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
package org.apache.drill.exec.hive.complex_types;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.apache.drill.categories.HiveStorageTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.exec.hive.HiveTestBase;
import org.apache.drill.exec.util.StoragePluginTestUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


import static org.apache.drill.exec.expr.fn.impl.DateUtility.parseBest;
import static org.apache.drill.exec.expr.fn.impl.DateUtility.parseLocalDate;
import static org.apache.drill.test.TestBuilder.mapOfObject;

@Category({SlowTest.class, HiveStorageTest.class})
public class TestHiveMaps extends HiveTestBase {

  @BeforeClass
  public static void generateTestData() throws Exception {
    String jdbcUrl = String.format("jdbc:hive2://%s:%d/default",
        HIVE_CONTAINER.getHost(),
        HIVE_CONTAINER.getMappedPort(10000));

    try (Connection conn = DriverManager.getConnection(jdbcUrl, "", "");
         Statement stmt = conn.createStatement()) {

      // Create simple map table
      stmt.execute("CREATE TABLE IF NOT EXISTS map_tbl(" +
          "rid INT, " +
          "int_string MAP<INT, STRING>," +
          "timestamp_decimal MAP<TIMESTAMP, DECIMAL(9,3)>," +
          "char_tinyint MAP<CHAR(2), TINYINT>," +
          "date_boolean MAP<DATE, BOOLEAN>," +
          "double_float MAP<DOUBLE, FLOAT>," +
          "varchar_bigint MAP<VARCHAR(5), BIGINT>," +
          "boolean_smallint MAP<BOOLEAN, SMALLINT>," +
          "decimal_char MAP<DECIMAL(9,3), CHAR(1)))");

      // Insert test data
      stmt.execute("INSERT INTO map_tbl VALUES " +
          "(1, map(1,'First',2,'Second',3,'Third'), " +
          "map(cast('2018-10-21 04:51:36' as timestamp),-100000.000,cast('2017-07-11 09:26:48' as timestamp),102030.001), " +
          "map('MN',-128,'MX',127,'ZR',0), " +
          "map(cast('1965-12-15' as date),true,cast('1970-09-02' as date),false,cast('2025-05-25' as date),true,cast('2919-01-17' as date),false), " +
          "map(0.47745359256854,-5.3763375), " +
          "map('m',-3226305034926780974), " +
          "map(true,-19088), " +
          "map(-3.930,'L'))");

      stmt.execute("INSERT INTO map_tbl VALUES " +
          "(2, map(4,'Fourth',5,'Fifth'), " +
          "map(cast('1913-03-03 18:47:14' as timestamp),84.509), " +
          "map('ls',1,'ks',2), " +
          "map(cast('1944-05-09' as date),false,cast('2002-02-11' as date),true), " +
          "map(-0.47745359256854,-0.6549191,-13.241563769628,-82.399826,0.3436367772981237,12.633938,9.73366,86.19402), " +
          "map('MBAv',0), " +
          "map(false,-4774), " +
          "map(-0.600,'P',21.555,'C',99.999,'X'))");

      stmt.execute("INSERT INTO map_tbl VALUES " +
          "(3, map(6,'Sixth',2,'!!'), " +
          "map(cast('2016-01-21 12:39:30' as timestamp),906668.849), " +
          "map('fx',20,'fy',30,'fz',40,'fk',-31), " +
          "map(cast('2068-10-05' as date),false,cast('2051-07-27' as date),false,cast('2052-08-28' as date),true), " +
          "map(170000000.00,9867.5623), " +
          "map('7R9F',-2077124879355227614,'12AAa',-6787493227661516341), " +
          "map(false,32767,true,25185), " +
          "map(-444023.971,'L',827746.528,'A'))");

      // Create Parquet table
      stmt.execute("CREATE TABLE IF NOT EXISTS map_tbl_p(" +
          "rid INT, " +
          "int_string MAP<INT, STRING>," +
          "timestamp_decimal MAP<TIMESTAMP, DECIMAL(9,3)>," +
          "char_tinyint MAP<CHAR(2), TINYINT>," +
          "date_boolean MAP<DATE, BOOLEAN>," +
          "double_float MAP<DOUBLE, FLOAT>," +
          "varchar_bigint MAP<VARCHAR(5), BIGINT>," +
          "boolean_smallint MAP<BOOLEAN, SMALLINT>," +
          "decimal_char MAP<DECIMAL(9,3), CHAR(1))) " +
          "STORED AS PARQUET");

      stmt.execute("INSERT INTO map_tbl_p SELECT * FROM map_tbl");

      // Create view
      stmt.execute("CREATE VIEW IF NOT EXISTS map_tbl_vw AS SELECT int_string FROM map_tbl WHERE rid=1");
    }
  }

  @Test
  public void mapIntToString() throws Exception {
    testBuilder()
        .sqlQuery("SELECT rid, int_string FROM hive.map_tbl")
        .unOrdered()
        .baselineColumns("rid", "int_string")
        .baselineValues(1, mapOfObject(1, "First", 2, "Second", 3, "Third"))
        .baselineValues(2, mapOfObject(4, "Fourth", 5, "Fifth"))
        .baselineValues(3, mapOfObject(6, "Sixth", 2, "!!"))
        .go();
  }

  @Test
  public void mapIntToStringInHiveView() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM hive.map_tbl_vw")
        .unOrdered()
        .baselineColumns("int_string")
        .baselineValues(mapOfObject(1, "First", 2, "Second", 3, "Third"))
        .go();
  }

  @Test
  public void mapIntToStringInDrillView() throws Exception {
    test(String.format(
        "CREATE VIEW %s.`map_vw` AS SELECT int_string FROM hive.map_tbl WHERE rid=1",
        StoragePluginTestUtils.DFS_TMP_SCHEMA
    ));
    testBuilder()
        .sqlQuery("SELECT * FROM %s.map_vw", StoragePluginTestUtils.DFS_TMP_SCHEMA)
        .unOrdered()
        .baselineColumns("int_string")
        .baselineValues(mapOfObject(1, "First", 2, "Second", 3, "Third"))
        .go();
  }

  @Test
  public void mapTimestampToDecimal() throws Exception {
    testBuilder()
        .sqlQuery("SELECT rid, timestamp_decimal FROM hive.map_tbl")
        .unOrdered()
        .baselineColumns("rid", "timestamp_decimal")
        .baselineValues(1, mapOfObject(
            parseBest("2018-10-21 04:51:36"), new BigDecimal("-100000.000"),
            parseBest("2017-07-11 09:26:48"), new BigDecimal("102030.001")
        ))
        .baselineValues(2, mapOfObject(
            parseBest("1913-03-03 18:47:14"), new BigDecimal("84.509")
        ))
        .baselineValues(3, mapOfObject(
            parseBest("2016-01-21 12:39:30"), new BigDecimal("906668.849")
        ))
        .go();
  }

  @Test
  public void mapCharToTinyint() throws Exception {
    testBuilder()
        .sqlQuery("SELECT rid, char_tinyint FROM hive.map_tbl")
        .unOrdered()
        .baselineColumns("rid", "char_tinyint")
        .baselineValues(1, mapOfObject("MN", -128, "MX", 127, "ZR", 0))
        .baselineValues(2, mapOfObject("ls", 1, "ks", 2))
        .baselineValues(3, mapOfObject("fx", 20, "fy", 30, "fz", 40, "fk", -31))
        .go();
  }

  @Test
  public void mapDateToBoolean() throws Exception {
    testBuilder()
        .sqlQuery("SELECT rid, date_boolean FROM hive.map_tbl")
        .unOrdered()
        .baselineColumns("rid", "date_boolean")
        .baselineValues(1, mapOfObject(
            parseLocalDate("1965-12-15"), true, parseLocalDate("1970-09-02"), false,
            parseLocalDate("2025-05-25"), true, parseLocalDate("2919-01-17"), false
        ))
        .baselineValues(2, mapOfObject(
            parseLocalDate("1944-05-09"), false, parseLocalDate("2002-02-11"), true
        ))
        .baselineValues(3, mapOfObject(
            parseLocalDate("2068-10-05"), false, parseLocalDate("2051-07-27"), false,
            parseLocalDate("2052-08-28"), true
        ))
        .go();
  }

  @Test
  public void mapDoubleToFloat() throws Exception {
    testBuilder()
        .sqlQuery("SELECT rid, double_float FROM hive.map_tbl")
        .unOrdered()
        .baselineColumns("rid", "double_float")
        .baselineValues(1, mapOfObject(
            0.47745359256854, -5.3763375f
        ))
        .baselineValues(2, mapOfObject(
            -0.47745359256854, -0.6549191f,
            -13.241563769628, -82.399826f,
            0.3436367772981237, 12.633938f,
            9.73366, 86.19402f
        ))
        .baselineValues(3, mapOfObject(
            170000000.00, 9867.5623f
        ))
        .go();
  }

  @Test
  public void mapVarcharToBigint() throws Exception {
    testBuilder()
        .sqlQuery("SELECT rid, varchar_bigint FROM hive.map_tbl")
        .unOrdered()
        .baselineColumns("rid", "varchar_bigint")
        .baselineValues(1, mapOfObject("m", -3226305034926780974L))
        .baselineValues(2, mapOfObject("MBAv", 0L))
        .baselineValues(3, mapOfObject("7R9F", -2077124879355227614L, "12AAa", -6787493227661516341L))
        .go();
  }

  @Test
  public void mapBooleanToSmallint() throws Exception {
    testBuilder()
        .sqlQuery("SELECT rid, boolean_smallint FROM hive.map_tbl")
        .unOrdered()
        .baselineColumns("rid", "boolean_smallint")
        .baselineValues(1, mapOfObject(true, -19088))
        .baselineValues(2, mapOfObject(false, -4774))
        .baselineValues(3, mapOfObject(false, 32767, true, 25185))
        .go();
  }

  @Test
  public void mapDecimalToChar() throws Exception {
    testBuilder()
        .sqlQuery("SELECT rid, decimal_char FROM hive.map_tbl")
        .unOrdered()
        .baselineColumns("rid", "decimal_char")
        .baselineValues(1, mapOfObject(
            new BigDecimal("-3.930"), "L"))
        .baselineValues(2, mapOfObject(
            new BigDecimal("-0.600"), "P", new BigDecimal("21.555"), "C", new BigDecimal("99.999"), "X"))
        .baselineValues(3, mapOfObject(
            new BigDecimal("-444023.971"), "L", new BigDecimal("827746.528"), "A"))
        .go();
  }

  @Test
  public void mapIntToStringParquet() throws Exception {
    testBuilder()
        .sqlQuery("SELECT rid, int_string FROM hive.map_tbl_p")
        .unOrdered()
        .baselineColumns("rid", "int_string")
        .baselineValues(1, mapOfObject(1, "First", 2, "Second", 3, "Third"))
        .baselineValues(2, mapOfObject(4, "Fourth", 5, "Fifth"))
        .baselineValues(3, mapOfObject(6, "Sixth", 2, "!!"))
        .go();
  }

  @Test
  public void countMapColumn() throws Exception {
    testBuilder()
        .sqlQuery("SELECT COUNT(int_string) AS cnt FROM hive.map_tbl")
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(3L)
        .go();
  }

  @Test
  public void typeOfFunctions() throws Exception {
    testBuilder()
        .sqlQuery("SELECT sqlTypeOf(%1$s) sto, typeOf(%1$s) to, modeOf(%1$s) mo, drillTypeOf(%1$s) dto " +
            "FROM hive.map_tbl LIMIT 1", "int_string")
        .unOrdered()
        .baselineColumns("sto", "to",                "mo",       "dto")
        .baselineValues( "MAP", "DICT<INT,VARCHAR>", "NOT NULL", "DICT")
        .go();
  }
}
