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
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.hive.HiveTestBase;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchemaBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.util.JsonStringHashMap;
import org.apache.drill.exec.util.StoragePluginTestUtils;
import org.apache.drill.exec.util.Text;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static java.util.Arrays.asList;
import static org.apache.drill.exec.expr.fn.impl.DateUtility.parseBest;
import static org.apache.drill.exec.expr.fn.impl.DateUtility.parseLocalDate;
import static org.apache.drill.test.TestBuilder.listOf;
import static org.apache.drill.test.TestBuilder.mapOf;
import static org.apache.drill.test.TestBuilder.mapOfObject;

@Category({SlowTest.class, HiveStorageTest.class})
public class TestHiveStructs extends HiveTestBase {

  private static final JsonStringHashMap<String, Object> STR_N0_ROW_1 = mapOf(
      "f_int", -3000, "f_string", new Text("AbbBBa"), "f_varchar", new Text("-c54g"), "f_char", new Text("Th"),
      "f_tinyint", -128, "f_smallint", -32768, "f_decimal", new BigDecimal("375098.406"), "f_boolean", true,
      "f_bigint", -9223372036854775808L, "f_float", -32.058f, "f_double", -13.241563769628,
      "f_date", parseLocalDate("2018-10-21"),
      "f_timestamp", parseBest("2018-10-21 04:51:36"));

  private static final JsonStringHashMap<String, Object> STR_N0_ROW_2 = mapOf(
      "f_int", 33000, "f_string", new Text("ZzZzZz"), "f_varchar", new Text("-+-+1"), "f_char", new Text("hh"),
      "f_tinyint", 127, "f_smallint", 32767, "f_decimal", new BigDecimal("500.500"), "f_boolean", true,
      "f_bigint", 798798798798798799L, "f_float", 102.058f, "f_double", 111.241563769628,
      "f_date", parseLocalDate("2019-10-21"),
      "f_timestamp", parseBest("2019-10-21 05:51:31"));

  private static final JsonStringHashMap<String, Object> STR_N0_ROW_3 = mapOf(
      "f_int", 9199, "f_string", new Text("z x cz"), "f_varchar", new Text(")(*1`"), "f_char", new Text("za"),
      "f_tinyint", 57, "f_smallint", 1010, "f_decimal", new BigDecimal("2.302"), "f_boolean", false,
      "f_bigint", 101010L, "f_float", 12.2001f, "f_double", 1.000000000001,
      "f_date", parseLocalDate("2010-01-01"),
      "f_timestamp", parseBest("2000-02-02 01:10:09"));

  private static final JsonStringHashMap<String, Object> STR_N2_ROW_1 = mapOf(
      "a", mapOf("b", mapOf("c", 1000, "k", "Z")));

  private static final JsonStringHashMap<String, Object> STR_N2_ROW_2 = mapOf(
      "a", mapOf("b", mapOf("c", 2000, "k", "X")));

  private static final JsonStringHashMap<String, Object> STR_N2_ROW_3 = mapOf(
      "a", mapOf("b", mapOf("c", 3000, "k", "C")));

  @BeforeClass
  public static void generateTestData() throws Exception {
    String jdbcUrl = String.format("jdbc:hive2://%s:%d/default",
        HIVE_CONTAINER.getHost(),
        HIVE_CONTAINER.getMappedPort(10000));

    try (Connection conn = DriverManager.getConnection(jdbcUrl, "", "");
         Statement stmt = conn.createStatement()) {

      // Create struct table with all complex nested types
      String structDdl = "CREATE TABLE IF NOT EXISTS struct_tbl(" +
          "rid INT, " +
          "str_n0 STRUCT<f_int:INT,f_string:STRING,f_varchar:VARCHAR(5),f_char:CHAR(2)," +
          "f_tinyint:TINYINT,f_smallint:SMALLINT,f_decimal:DECIMAL(9,3),f_boolean:BOOLEAN," +
          "f_bigint:BIGINT,f_float:FLOAT,f_double:DOUBLE,f_date:DATE,f_timestamp:TIMESTAMP>, " +
          "str_n1 STRUCT<sid:INT,coord:STRUCT<x:TINYINT,y:CHAR(1)>>, " +
          "str_n2 STRUCT<a:STRUCT<b:STRUCT<c:INT,k:CHAR(1)>>>, " +
          "str_wa STRUCT<t:INT,a:ARRAY<INT>,a2:ARRAY<ARRAY<INT>>>, " +
          "str_map STRUCT<i:INT, m:MAP<INT, INT>, sm:MAP<STRING,INT>>, " +
          "str_wa_2 STRUCT<fn:INT,fa:ARRAY<STRUCT<sn:INT,sa:ARRAY<STRUCT<tn:INT,ts:STRING>>>>>) " +
          "STORED AS ORC";
      stmt.execute(structDdl);

      // Insert row 1
      stmt.execute("INSERT INTO struct_tbl VALUES (" +
          "1, " +
          "named_struct(" +
          "'f_int',-3000,'f_string','AbbBBa','f_varchar','-c54g','f_char','Th'," +
          "'f_tinyint',-128,'f_smallint',-32768,'f_decimal',375098.406,'f_boolean',true," +
          "'f_bigint',-9223372036854775808,'f_float',-32.058,'f_double',-13.241563769628," +
          "'f_date',CAST('2018-10-21' AS DATE),'f_timestamp',CAST('2018-10-21 04:51:36' AS TIMESTAMP)), " +
          "named_struct('sid',1,'coord',named_struct('x',1,'y','A')), " +
          "named_struct('a',named_struct('b',named_struct('c',1000,'k','Z'))), " +
          "named_struct('t',1,'a',array(-1,1,-2,2),'a2',array(array(1,2,3,4),array(0,-1,-2))), " +
          "named_struct('i',1,'m',map(1,0,0,1),'sm',map('a',0)), " +
          "named_struct('fn',1,'fa',array(" +
          "named_struct('sn',10,'sa',array(named_struct('tn',1000,'ts','s1'),named_struct('tn',2000,'ts','s2'),named_struct('tn',3000,'ts','s3')))," +
          "named_struct('sn',20,'sa',array(named_struct('tn',4000,'ts','s4'),named_struct('tn',5000,'ts','s5')))," +
          "named_struct('sn',30,'sa',array(named_struct('tn',6000,'ts','s6'))))))");

      // Insert row 2
      stmt.execute("INSERT INTO struct_tbl VALUES (" +
          "2, " +
          "named_struct(" +
          "'f_int',33000,'f_string','ZzZzZz','f_varchar','-+-+1','f_char','hh'," +
          "'f_tinyint',127,'f_smallint',32767,'f_decimal',500.500,'f_boolean',true," +
          "'f_bigint',798798798798798799,'f_float',102.058,'f_double',111.241563769628," +
          "'f_date',CAST('2019-10-21' AS DATE),'f_timestamp',CAST('2019-10-21 05:51:31' AS TIMESTAMP)), " +
          "named_struct('sid',2,'coord',named_struct('x',2,'y','B')), " +
          "named_struct('a',named_struct('b',named_struct('c',2000,'k','X'))), " +
          "named_struct('t',2,'a',array(-11,11,-12,12),'a2',array(array(1,2),array(-1),array(1,1,1))), " +
          "named_struct('i',2,'m',map(1,3,2,2),'sm',map('a',-1)), " +
          "named_struct('fn',2,'fa',array(" +
          "named_struct('sn',40,'sa',array(named_struct('tn',7000,'ts','s7'),named_struct('tn',8000,'ts','s8')))," +
          "named_struct('sn',50,'sa',array(named_struct('tn',9000,'ts','s9'))))))");

      // Insert row 3
      stmt.execute("INSERT INTO struct_tbl VALUES (" +
          "3, " +
          "named_struct(" +
          "'f_int',9199,'f_string','z x cz','f_varchar',')(*1`','f_char','za'," +
          "'f_tinyint',57,'f_smallint',1010,'f_decimal',2.302,'f_boolean',false," +
          "'f_bigint',101010,'f_float',12.2001,'f_double',1.000000000001," +
          "'f_date',CAST('2010-01-01' AS DATE),'f_timestamp',CAST('2000-02-02 01:10:09' AS TIMESTAMP)), " +
          "named_struct('sid',3,'coord',named_struct('x',3,'y','C')), " +
          "named_struct('a',named_struct('b',named_struct('c',3000,'k','C'))), " +
          "named_struct('t',3,'a',array(0,0,0),'a2',array(array(0,0),array(0,0,0,0,0,0))), " +
          "named_struct('i',3,'m',map(1,4,2,3,0,5),'sm',map('a',-2)), " +
          "named_struct('fn',3,'fa',array(" +
          "named_struct('sn',60,'sa',array(named_struct('tn',10000,'ts','s10'))))))");

      // Create Parquet table
      String structDdlP = "CREATE TABLE IF NOT EXISTS struct_tbl_p(" +
          "rid INT, " +
          "str_n0 STRUCT<f_int:INT,f_string:STRING,f_varchar:VARCHAR(5),f_char:CHAR(2)," +
          "f_tinyint:TINYINT,f_smallint:SMALLINT,f_decimal:DECIMAL(9,3),f_boolean:BOOLEAN," +
          "f_bigint:BIGINT,f_float:FLOAT,f_double:DOUBLE,f_date:DATE,f_timestamp:TIMESTAMP>, " +
          "str_n1 STRUCT<sid:INT,coord:STRUCT<x:TINYINT,y:CHAR(1)>>, " +
          "str_n2 STRUCT<a:STRUCT<b:STRUCT<c:INT,k:CHAR(1)>>>, " +
          "str_wa STRUCT<t:INT,a:ARRAY<INT>,a2:ARRAY<ARRAY<INT>>>, " +
          "str_map STRUCT<i:INT, m:MAP<INT, INT>, sm:MAP<STRING,INT>>, " +
          "str_wa_2 STRUCT<fn:INT,fa:ARRAY<STRUCT<sn:INT,sa:ARRAY<STRUCT<tn:INT,ts:STRING>>>>>) " +
          "STORED AS PARQUET";
      stmt.execute(structDdlP);
      stmt.execute("INSERT INTO struct_tbl_p SELECT * FROM struct_tbl");

      // Create view
      String hiveViewDdl = "CREATE VIEW IF NOT EXISTS struct_tbl_vw " +
          "AS SELECT str_n0.f_int AS fint, str_n1.coord AS cord, str_wa AS wizarr " +
          "FROM struct_tbl WHERE rid=1";
      stmt.execute(hiveViewDdl);

      // Create struct_union_tbl
      String structUnionDdl = "CREATE TABLE IF NOT EXISTS " +
          "struct_union_tbl(rid INT, str_u STRUCT<n:INT,u:UNIONTYPE<INT,STRING>>) " +
          "ROW FORMAT DELIMITED" +
          " FIELDS TERMINATED BY ','" +
          " COLLECTION ITEMS TERMINATED BY '&'" +
          " MAP KEYS TERMINATED BY '#'" +
          " LINES TERMINATED BY '\\n'" +
          " STORED AS TEXTFILE";
      stmt.execute(structUnionDdl);

      // Create dummy table to generate union data
      stmt.execute("CREATE TABLE IF NOT EXISTS dummy_union(d INT) STORED AS TEXTFILE");
      stmt.execute("INSERT INTO dummy_union VALUES (1)");

      // Insert struct_union rows
      stmt.execute("INSERT INTO struct_union_tbl SELECT 1, named_struct('n',-3,'u',create_union(0,1000,'Text')) FROM dummy_union");
      stmt.execute("INSERT INTO struct_union_tbl SELECT 2, named_struct('n',5,'u',create_union(1,1000,'Text')) FROM dummy_union");
    }
  }

  @Test
  public void nestedStruct() throws Exception {
    testBuilder()
        .sqlQuery("SELECT str_n1 FROM hive.struct_tbl ORDER BY rid")
        .ordered()
        .baselineColumns("str_n1")
        .baselineValues(mapOf("sid", 1, "coord", mapOf("x", 1, "y", "A")))
        .baselineValues(mapOf("sid", 2, "coord", mapOf("x", 2, "y", "B")))
        .baselineValues(mapOf("sid", 3, "coord", mapOf("x", 3, "y", "C")))
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
        .baselineValues(mapOf("c", 1000, "k", "Z"))
        .baselineValues(mapOf("c", 2000, "k", "X"))
        .baselineValues(mapOf("c", 3000, "k", "C"))
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

  @Test // DRILL-7429
  public void testCorrectColumnOrdering() throws Exception {
    BatchSchema expectedSchema = new BatchSchemaBuilder()
        .withSchemaBuilder(new SchemaBuilder()
            .addMap("a").resumeSchema()
            .addNullable("b", TypeProtos.MinorType.INT))
        .build();

    String sql = "SELECT t.str_n0 a, rid b FROM hive.struct_tbl t LIMIT 1";
    testBuilder()
        .sqlQuery(sql)
        .schemaBaseLine(expectedSchema)
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
            mapOf("t", 1, "a", asList(-1, 1, -2, 2), "a2", asList(asList(1, 2, 3, 4), asList(0, -1, -2)))
        )
        .baselineValues(2,
            mapOf("t", 2, "a", asList(-11, 11, -12, 12), "a2", asList(asList(1, 2), asList(-1), asList(1, 1, 1)))
        )
        .baselineValues(3,
            mapOf("t", 3, "a", asList(0, 0, 0), "a2", asList(asList(0, 0), asList(0, 0, 0, 0, 0, 0)))
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
  public void structWithMapFieldAccess() throws Exception {
    testBuilder()
        .sqlQuery("SELECT rid, st.str_map.m FROM hive.struct_tbl st ORDER BY rid")
        .ordered()
        .baselineColumns("rid", "EXPR$1")
        .baselineValues(1, mapOfObject(1, 0, 0, 1))
        .baselineValues(2, mapOfObject(1, 3, 2, 2))
        .baselineValues(3, mapOfObject(1, 4, 2, 3, 0, 5))
        .go();
  }

  @Test
  public void primitiveStructParquet() throws Exception {
    testBuilder()
        .sqlQuery("SELECT str_n0 FROM hive.struct_tbl_p")
        .unOrdered()
        .baselineColumns("str_n0")
        .baselineValues(STR_N0_ROW_1)
        .baselineValues(STR_N0_ROW_2)
        .baselineValues(STR_N0_ROW_3)
        .go();
  }

  @Test
  public void viewWithStructs() throws Exception {
    testBuilder()
        .sqlQuery("SELECT fint, cord, wizarr FROM hive.struct_tbl_vw")
        .unOrdered()
        .baselineColumns("fint", "cord", "wizarr")
        .baselineValues(-3000, mapOf("x", 1, "y", "A"),
            mapOf("t", 1, "a", asList(-1, 1, -2, 2), "a2", asList(asList(1, 2, 3, 4), asList(0, -1, -2))))
        .go();
  }

  @Test
  public void drillViewWithStructs() throws Exception {
    test(String.format(
        "CREATE VIEW %s.`struct_vw` AS SELECT str_n0 FROM hive.struct_tbl WHERE rid=1",
        StoragePluginTestUtils.DFS_TMP_SCHEMA
    ));
    testBuilder()
        .sqlQuery("SELECT * FROM %s.struct_vw", StoragePluginTestUtils.DFS_TMP_SCHEMA)
        .unOrdered()
        .baselineColumns("str_n0")
        .baselineValues(STR_N0_ROW_1)
        .go();
  }

  @Test
  public void structWithUnion() throws Exception {
    testBuilder()
        .sqlQuery("SELECT rid, str_u.n, str_u.u FROM hive.struct_union_tbl ORDER BY rid")
        .ordered()
        .baselineColumns("rid", "EXPR$1", "EXPR$2")
        .baselineValues(1, -3, 1000)
        .baselineValues(2, 5, "Text")
        .go();
  }

  @Test
  public void typeOfFunctions() throws Exception {
    testBuilder()
        .sqlQuery("SELECT sqlTypeOf(%1$s) sto, typeOf(%1$s) to, modeOf(%1$s) mo, drillTypeOf(%1$s) dto " +
            "FROM hive.struct_tbl LIMIT 1", "str_n0")
        .unOrdered()
        .baselineColumns("sto", "to", "mo", "dto")
        .baselineValues("STRUCT", "MAP", "NOT NULL", "MAP")
        .go();
  }
}
