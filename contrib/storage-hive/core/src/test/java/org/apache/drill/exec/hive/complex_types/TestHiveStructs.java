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
import java.nio.file.Paths;

import org.apache.drill.categories.HiveStorageTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.hive.HiveTestFixture;
import org.apache.drill.exec.hive.HiveTestUtilities;
import org.apache.drill.exec.util.JsonStringHashMap;
import org.apache.drill.exec.util.StoragePluginTestUtils;
import org.apache.drill.exec.util.Text;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.TestBuilder;
import org.apache.hadoop.hive.ql.Driver;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static java.util.Arrays.asList;
import static org.apache.drill.exec.expr.fn.impl.DateUtility.parseBest;
import static org.apache.drill.exec.expr.fn.impl.DateUtility.parseLocalDate;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

@Category({SlowTest.class, HiveStorageTest.class})
public class TestHiveStructs extends ClusterTest {

  private static final JsonStringHashMap<String, Object> STR_N0_ROW_1 = TestBuilder.mapOf(
      "f_int", -3000, "f_string", new Text("AbbBBa"), "f_varchar", new Text("-c54g"), "f_char", new Text("Th"),
      "f_tinyint", -128, "f_smallint", -32768, "f_decimal", new BigDecimal("375098.406"), "f_boolean", true,
      "f_bigint", -9223372036854775808L, "f_float", -32.058f, "f_double", -13.241563769628,
      "f_date", parseLocalDate("2018-10-21"),
      "f_timestamp", parseBest("2018-10-21 04:51:36"));

  private static final JsonStringHashMap<String, Object> STR_N0_ROW_2 = TestBuilder.mapOf(
      "f_int", 33000, "f_string", new Text("ZzZzZz"), "f_varchar", new Text("-+-+1"), "f_char", new Text("hh"),
      "f_tinyint", 127, "f_smallint", 32767, "f_decimal", new BigDecimal("500.500"), "f_boolean", true,
      "f_bigint", 798798798798798799L, "f_float", 102.058f, "f_double", 111.241563769628,
      "f_date", parseLocalDate("2019-10-21"),
      "f_timestamp", parseBest("2019-10-21 05:51:31"));

  private static final JsonStringHashMap<String, Object> STR_N0_ROW_3 = TestBuilder.mapOf(
      "f_int", 9199, "f_string", new Text("z x cz"), "f_varchar", new Text(")(*1`"), "f_char", new Text("za"),
      "f_tinyint", 57, "f_smallint", 1010, "f_decimal", new BigDecimal("2.302"), "f_boolean", false,
      "f_bigint", 101010L, "f_float", 12.2001f, "f_double", 1.000000000001,
      "f_date", parseLocalDate("2010-01-01"),
      "f_timestamp", parseBest("2000-02-02 01:10:09"));

  private static final JsonStringHashMap<String, Object> STR_N2_ROW_1 = TestBuilder.mapOf("a",
      TestBuilder.mapOf("b", TestBuilder.mapOf("c", 1000, "k", "Z")));

  private static final JsonStringHashMap<String, Object> STR_N2_ROW_2 = TestBuilder.mapOf(
      "a", TestBuilder.mapOf("b", TestBuilder.mapOf("c", 2000, "k", "X")));

  private static final JsonStringHashMap<String, Object> STR_N2_ROW_3 = TestBuilder.mapOf(
      "a", TestBuilder.mapOf("b", TestBuilder.mapOf("c", 3000, "k", "C")));

  private static HiveTestFixture hiveTestFixture;

  @BeforeClass
  public static void setUp() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher)
        .sessionOption(ExecConstants.HIVE_OPTIMIZE_PARQUET_SCAN_WITH_NATIVE_READER, true));
    hiveTestFixture = HiveTestFixture.builder(dirTestWatcher).build();
    hiveTestFixture.getDriverManager().runWithinSession(TestHiveStructs::generateData);
    hiveTestFixture.getPluginManager().addHivePluginTo(cluster.drillbit());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (hiveTestFixture != null) {
      hiveTestFixture.getPluginManager().removeHivePluginFrom(cluster.drillbit());
    }
  }

  private static void generateData(Driver d) {
    String structDdl = "CREATE TABLE struct_tbl(" +
        "rid INT, " +
        "str_n0 STRUCT<f_int:INT,f_string:STRING,f_varchar:VARCHAR(5),f_char:CHAR(2)," +
        "f_tinyint:TINYINT,f_smallint:SMALLINT,f_decimal:DECIMAL(9,3),f_boolean:BOOLEAN," +
        "f_bigint:BIGINT,f_float:FLOAT,f_double:DOUBLE,f_date:DATE,f_timestamp:TIMESTAMP>, " +
        "str_n1 STRUCT<sid:INT,coord:STRUCT<x:TINYINT,y:CHAR(1)>>, " +
        "str_n2 STRUCT<a:STRUCT<b:STRUCT<c:INT,k:CHAR(1)>>>, " +
        "str_wa STRUCT<t:INT,a:ARRAY<INT>,a2:ARRAY<ARRAY<INT>>>" +
        ") " +
        "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE";
    HiveTestUtilities.executeQuery(d, structDdl);
    HiveTestUtilities.loadData(d, "struct_tbl", Paths.get("complex_types/struct/struct_tbl.json"));

    String structDdlP = "CREATE TABLE struct_tbl_p(" +
        "rid INT, " +
        "str_n0 STRUCT<f_int:INT,f_string:STRING,f_varchar:VARCHAR(5),f_char:CHAR(2)," +
        "f_tinyint:TINYINT,f_smallint:SMALLINT,f_decimal:DECIMAL(9,3),f_boolean:BOOLEAN," +
        "f_bigint:BIGINT,f_float:FLOAT,f_double:DOUBLE,f_date:DATE,f_timestamp:TIMESTAMP>, " +
        "str_n1 STRUCT<sid:INT,coord:STRUCT<x:TINYINT,y:CHAR(1)>>, " +
        "str_n2 STRUCT<a:STRUCT<b:STRUCT<c:INT,k:CHAR(1)>>>, " +
        "str_wa STRUCT<t:INT,a:ARRAY<INT>,a2:ARRAY<ARRAY<INT>>>" +
        ") " +
        "STORED AS PARQUET";
    HiveTestUtilities.executeQuery(d, structDdlP);
    HiveTestUtilities.insertData(d, "struct_tbl", "struct_tbl_p");

    String hiveViewDdl = "CREATE VIEW struct_tbl_vw " +
        "AS SELECT str_n0.f_int AS fint, str_n1.coord AS cord, str_wa AS wizarr " +
        "FROM struct_tbl WHERE rid=1";
    HiveTestUtilities.executeQuery(d, hiveViewDdl);
  }

  @Test
  public void nestedStruct() throws Exception {
    testBuilder()
        .sqlQuery("SELECT str_n1 FROM hive.struct_tbl ORDER BY rid")
        .ordered()
        .baselineColumns("str_n1")
        .baselineValues(TestBuilder.mapOf(
            "sid", 1,
            "coord", TestBuilder.mapOf("x", 1, "y", "A")
        ))
        .baselineValues(TestBuilder.mapOf(
            "sid", 2,
            "coord", TestBuilder.mapOf("x", 2, "y", "B")
        ))
        .baselineValues(TestBuilder.mapOf(
            "sid", 3,
            "coord", TestBuilder.mapOf("x", 3, "y", "C")
        ))
        .go();
  }

  @Test
  public void doublyNestedStruct() throws Exception {
    testBuilder()
        .sqlQuery("SELECT str_n2 FROM hive.struct_tbl ORDER BY rid")
        .ordered()
        .baselineColumns("str_n2")
        .baselineValues(STR_N2_ROW_1)
        .baselineValues(STR_N2_ROW_2)
        .baselineValues(STR_N2_ROW_3)
        .go();
  }

  @Test
  public void nestedStructAccessPrimitiveField() throws Exception {
    testBuilder()
        .sqlQuery("SELECT ns.str_n2.a.b.k AS abk " +
            "FROM hive.struct_tbl ns")
        .unOrdered()
        .baselineColumns("abk")
        .baselineValues("Z")
        .baselineValues("X")
        .baselineValues("C")
        .go();
  }

  @Test
  public void nestedStructAccessStructField() throws Exception {
    testBuilder()
        .sqlQuery("SELECT ns.str_n2.a.b AS ab " +
            "FROM hive.struct_tbl ns")
        .unOrdered()
        .baselineColumns("ab")
        .baselineValues(TestBuilder.mapOf("c", 1000, "k", "Z"))
        .baselineValues(TestBuilder.mapOf("c", 2000, "k", "X"))
        .baselineValues(TestBuilder.mapOf("c", 3000, "k", "C"))
        .go();
  }

  @Test
  public void primitiveStructWithFilter() throws Exception {
    testBuilder()
        .sqlQuery("SELECT str_n0 FROM hive.struct_tbl WHERE rid=1")
        .unOrdered()
        .baselineColumns("str_n0")
        .baselineValues(STR_N0_ROW_1)
        .go();
  }

  @Test
  public void primitiveStructFieldAccess() throws Exception {
    testBuilder()
        .sqlQuery("SELECT " +
            "t.str_n0.f_int as f_int, " +
            "t.str_n0.f_string as f_string, " +
            "t.str_n0.f_varchar as f_varchar, " +
            "t.str_n0.f_char as f_char, " +
            "t.str_n0.f_tinyint as f_tinyint, " +
            "t.str_n0.f_smallint as f_smallint, " +
            "t.str_n0.f_decimal as f_decimal" +
            " FROM hive.struct_tbl t")
        .unOrdered()
        .baselineColumns("f_int", "f_string", "f_varchar", "f_char", "f_tinyint", "f_smallint", "f_decimal")
        .baselineValues(-3000, "AbbBBa", "-c54g", "Th", -128, -32768, new BigDecimal("375098.406"))
        .baselineValues(33000, "ZzZzZz", "-+-+1", "hh", 127, 32767, new BigDecimal("500.500"))
        .baselineValues(9199, "z x cz", ")(*1`", "za", 57, 1010, new BigDecimal("2.302"))
        .go();
  }

  @Test
  public void primitiveStruct() throws Exception {
    testBuilder()
        .sqlQuery("SELECT str_n0 FROM hive.struct_tbl")
        .unOrdered()
        .baselineColumns("str_n0")
        .baselineValues(STR_N0_ROW_1)
        .baselineValues(STR_N0_ROW_2)
        .baselineValues(STR_N0_ROW_3)
        .go();
  }

  @Test
  public void primitiveStructWithOrdering() throws Exception {
    testBuilder()
        .sqlQuery("SELECT str_n0 FROM hive.struct_tbl ORDER BY rid DESC")
        .ordered()
        .baselineColumns("str_n0")
        .baselineValues(STR_N0_ROW_3)
        .baselineValues(STR_N0_ROW_2)
        .baselineValues(STR_N0_ROW_1)
        .go();
  }

  @Test
  public void structWithArr() throws Exception {
    testBuilder()
        .sqlQuery("SELECT rid, str_wa FROM hive.struct_tbl ORDER BY rid")
        .ordered()
        .baselineColumns("rid", "str_wa")
        .baselineValues(1,
            TestBuilder.mapOf("t", 1, "a", asList(-1, 1, -2, 2), "a2", asList(asList(1, 2, 3, 4), asList(0, -1, -2)))
        )
        .baselineValues(2,
            TestBuilder.mapOf("t", 2, "a", asList(-11, 11, -12, 12), "a2", asList(asList(1, 2), asList(-1), asList(1, 1, 1)))
        )
        .baselineValues(3,
            TestBuilder.mapOf("t", 3, "a", asList(0, 0, 0), "a2", asList(asList(0, 0), asList(0, 0, 0, 0, 0, 0)))
        )
        .go();
  }

  @Test
  public void structWithArrFieldAccess() throws Exception {
    testBuilder()
        .sqlQuery("SELECT rid, st.str_wa.a FROM hive.struct_tbl st ORDER BY rid")
        .ordered()
        .baselineColumns("rid", "EXPR$1")
        .baselineValues(1, asList(-1, 1, -2, 2))
        .baselineValues(2, asList(-11, 11, -12, 12))
        .baselineValues(3, asList(0, 0, 0))
        .go();
  }

  @Test
  public void structWithArrFieldAccessByIdx() throws Exception {
    testBuilder()
        .sqlQuery("SELECT rid, st.str_wa.a[2] FROM hive.struct_tbl st ORDER BY rid")
        .ordered()
        .baselineColumns("rid", "EXPR$1")
        .baselineValues(1, -2)
        .baselineValues(2, -12)
        .baselineValues(3, 0)
        .go();
  }


  @Test
  public void primitiveStructParquet() throws Exception {
    checkNativeScanUsed("struct_tbl_p");
    testBuilder()
        .sqlQuery("SELECT str_n0 FROM hive.struct_tbl_p")
        .optionSettingQueriesForTestQuery("alter session set `" + ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP + "` = true")
        .unOrdered()
        .baselineColumns("str_n0")
        .baselineValues(STR_N0_ROW_1)
        .baselineValues(STR_N0_ROW_2)
        .baselineValues(STR_N0_ROW_3)
        .go();
  }

  @Test
  public void primitiveStructFilterByInnerField() throws Exception {
    testBuilder()
        .sqlQuery("SELECT rid FROM hive.struct_tbl st WHERE st.str_n0.f_int = -3000")
        .unOrdered()
        .baselineColumns("rid")
        .baselineValues(1)
        .go();
  }

  @Test
  public void primitiveStructOrderByInnerField() throws Exception {
    testBuilder()
        .sqlQuery("SELECT rid FROM hive.struct_tbl st ORDER BY st.str_n0.f_int")
        .unOrdered()
        .baselineColumns("rid")
        .baselineValues(1)
        .baselineValues(3)
        .baselineValues(2)
        .go();
  }

  @Test
  public void structInHiveView() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM hive.struct_tbl_vw")
        .unOrdered()
        .baselineColumns("fint", "cord", "wizarr")
        .baselineValues(-3000, TestBuilder.mapOf("x", 1, "y", "A"),
            TestBuilder.mapOf("t", 1, "a", asList(-1, 1, -2, 2), "a2", asList(asList(1, 2, 3, 4), asList(0, -1, -2))))
        .go();
  }

  @Test
  public void structInDrillView() throws Exception {
    String drillViewDdl = "CREATE VIEW " + StoragePluginTestUtils.DFS_TMP_SCHEMA + ".`str_vw` " +
        "AS SELECT s.str_n0.f_int AS fint, s.str_n1.coord AS cord, s.str_wa AS wizarr " +
        "FROM hive.struct_tbl s WHERE rid=1";
    queryBuilder().sql(drillViewDdl).run();

    testBuilder()
        .sqlQuery("SELECT * FROM dfs.tmp.`str_vw`")
        .unOrdered()
        .baselineColumns("fint", "cord", "wizarr")
        .baselineValues(-3000, TestBuilder.mapOf("x", 1, "y", "A"),
            TestBuilder.mapOf("t", 1, "a", asList(-1, 1, -2, 2), "a2", asList(asList(1, 2, 3, 4), asList(0, -1, -2))))
        .go();
  }

  private void checkNativeScanUsed(String table) throws Exception {
    String plan = queryBuilder().sql("SELECT * FROM hive.`%s`", table).explainText();
    assertThat(plan, containsString("HiveDrillNativeParquetScan"));
  }

}
