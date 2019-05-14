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
import java.util.Arrays;
import java.util.Collections;

import org.apache.drill.categories.HiveStorageTest;
import org.apache.drill.exec.hive.HiveTestFixture;
import org.apache.drill.exec.hive.HiveTestUtilities;
import org.apache.drill.exec.util.StoragePluginTestUtils;
import org.apache.drill.exec.util.Text;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.hadoop.hive.ql.Driver;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.drill.exec.expr.fn.impl.DateUtility.parseBest;
import static org.apache.drill.exec.expr.fn.impl.DateUtility.parseLocalDate;

@Category({HiveStorageTest.class})
public class TestHiveArrays extends ClusterTest {

  private static HiveTestFixture hiveTestFixture;

  @BeforeClass
  public static void setUp() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    hiveTestFixture = HiveTestFixture.builder(dirTestWatcher).build();
    hiveTestFixture.getDriverManager().runWithinSession(TestHiveArrays::generateData);
    hiveTestFixture.getPluginManager().addHivePluginTo(cluster.drillbit());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (hiveTestFixture != null) {
      hiveTestFixture.getPluginManager().removeHivePluginFrom(cluster.drillbit());
    }
  }

  private static void generateData(Driver d) {
    // int_array
    HiveTestUtilities.executeQuery(d,
        "CREATE TABLE int_array(rid INT, arr_n_0 ARRAY<INT>, arr_n_1 ARRAY<ARRAY<INT>>,arr_n_2 ARRAY<ARRAY<ARRAY<INT>>>) " +
            "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE");
    HiveTestUtilities.loadData(d, "int_array", Paths.get("complex_types/array/int_array.json"));

    // string_array
    HiveTestUtilities.executeQuery(d,
        "CREATE TABLE string_array(rid INT, arr_n_0 ARRAY<STRING>, arr_n_1 ARRAY<ARRAY<STRING>>,arr_n_2 ARRAY<ARRAY<ARRAY<STRING>>>) " +
            "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE");
    HiveTestUtilities.loadData(d, "string_array", Paths.get("complex_types/array/string_array.json"));

    // varchar_array
    HiveTestUtilities.executeQuery(d,
        "CREATE TABLE varchar_array(rid INT, arr_n_0 ARRAY<VARCHAR(5)>,arr_n_1 ARRAY<ARRAY<VARCHAR(5)>>,arr_n_2 ARRAY<ARRAY<ARRAY<VARCHAR(5)>>>) " +
            "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE");
    HiveTestUtilities.loadData(d, "varchar_array", Paths.get("complex_types/array/varchar_array.json"));

    // char_array
    HiveTestUtilities.executeQuery(d,
        "CREATE TABLE char_array(rid INT, arr_n_0 ARRAY<CHAR(2)>,arr_n_1 ARRAY<ARRAY<CHAR(2)>>, arr_n_2 ARRAY<ARRAY<ARRAY<CHAR(2)>>>) " +
            "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE");
    HiveTestUtilities.loadData(d, "char_array", Paths.get("complex_types/array/char_array.json"));

    // tinyint_array
    HiveTestUtilities.executeQuery(d,
        "CREATE TABLE tinyint_array(rid INT, arr_n_0 ARRAY<TINYINT>, arr_n_1 ARRAY<ARRAY<TINYINT>>, arr_n_2 ARRAY<ARRAY<ARRAY<TINYINT>>> ) " +
            "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE");
    HiveTestUtilities.loadData(d, "tinyint_array", Paths.get("complex_types/array/tinyint_array.json"));

    // smallint_array
    HiveTestUtilities.executeQuery(d,
        "CREATE TABLE smallint_array(rid INT, arr_n_0 ARRAY<SMALLINT>, arr_n_1 ARRAY<ARRAY<SMALLINT>>, arr_n_2 ARRAY<ARRAY<ARRAY<SMALLINT>>>) " +
            "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE");
    HiveTestUtilities.loadData(d, "smallint_array", Paths.get("complex_types/array/smallint_array.json"));

    // decimal_array
    HiveTestUtilities.executeQuery(d,
        "CREATE TABLE decimal_array(rid INT, arr_n_0 ARRAY<DECIMAL(9,3)>, arr_n_1 ARRAY<ARRAY<DECIMAL(9,3)>>,arr_n_2 ARRAY<ARRAY<ARRAY<DECIMAL(9,3)>>>) " +
            "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE");
    HiveTestUtilities.loadData(d, "decimal_array", Paths.get("complex_types/array/decimal_array.json"));

    // boolean_array
    HiveTestUtilities.executeQuery(d,
        "CREATE TABLE boolean_array(rid INT, arr_n_0 ARRAY<BOOLEAN>, arr_n_1 ARRAY<ARRAY<BOOLEAN>>,arr_n_2 ARRAY<ARRAY<ARRAY<BOOLEAN>>>) " +
            "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE");
    HiveTestUtilities.loadData(d, "boolean_array", Paths.get("complex_types/array/boolean_array.json"));

    // bigint_array
    HiveTestUtilities.executeQuery(d,
        "CREATE TABLE bigint_array(rid INT, arr_n_0 ARRAY<BIGINT>, arr_n_1 ARRAY<ARRAY<BIGINT>>,arr_n_2 ARRAY<ARRAY<ARRAY<BIGINT>>>) " +
            "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE");
    HiveTestUtilities.loadData(d, "bigint_array", Paths.get("complex_types/array/bigint_array.json"));

    // float_array
    HiveTestUtilities.executeQuery(d,
        "CREATE TABLE float_array(rid INT, arr_n_0 ARRAY<FLOAT>, arr_n_1 ARRAY<ARRAY<FLOAT>>,arr_n_2 ARRAY<ARRAY<ARRAY<FLOAT>>>) " +
            "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE");
    HiveTestUtilities.loadData(d, "float_array", Paths.get("complex_types/array/float_array.json"));

    // double_array
    HiveTestUtilities.executeQuery(d,
        "CREATE TABLE double_array(rid INT, arr_n_0 ARRAY<DOUBLE>, arr_n_1 ARRAY<ARRAY<DOUBLE>>, arr_n_2 ARRAY<ARRAY<ARRAY<DOUBLE>>>) " +
            "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE");
    HiveTestUtilities.loadData(d, "double_array", Paths.get("complex_types/array/double_array.json"));

    // date_array
    HiveTestUtilities.executeQuery(d,
        "CREATE TABLE date_array(rid INT, arr_n_0 ARRAY<DATE>, arr_n_1 ARRAY<ARRAY<DATE>>,arr_n_2 ARRAY<ARRAY<ARRAY<DATE>>>) " +
            "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE");
    HiveTestUtilities.loadData(d, "date_array", Paths.get("complex_types/array/date_array.json"));

    // timestamp_array
    HiveTestUtilities.executeQuery(d,
        "CREATE TABLE timestamp_array(rid INT, arr_n_0 ARRAY<TIMESTAMP>, arr_n_1 ARRAY<ARRAY<TIMESTAMP>>,arr_n_2 ARRAY<ARRAY<ARRAY<TIMESTAMP>>>) " +
            "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS TEXTFILE");
    HiveTestUtilities.loadData(d, "timestamp_array", Paths.get("complex_types/array/timestamp_array.json"));

    // binary_array
    HiveTestUtilities.executeQuery(d,
        "CREATE TABLE binary_array(arr_n_0 ARRAY<BINARY>) STORED AS TEXTFILE");
    HiveTestUtilities.executeQuery(d, "insert into binary_array select array(binary('First'),binary('Second'),binary('Third'))");
    HiveTestUtilities.executeQuery(d, "insert into binary_array select array(binary('First'))");

    // arr_hive_view
    HiveTestUtilities.executeQuery(d, "CREATE VIEW arr_view AS " +
        "SELECT " +
        "   int_array.rid as vwrid," +
        "   int_array.arr_n_0 as int_n0," +
        "   int_array.arr_n_1 as int_n1," +
        "   string_array.arr_n_0 as string_n0," +
        "   string_array.arr_n_1 as string_n1," +
        "   varchar_array.arr_n_0 as varchar_n0," +
        "   varchar_array.arr_n_1 as varchar_n1," +
        "   char_array.arr_n_0 as char_n0," +
        "   char_array.arr_n_1 as char_n1," +
        "   tinyint_array.arr_n_0 as tinyint_n0," +
        "   tinyint_array.arr_n_1 as tinyint_n1," +
        "   smallint_array.arr_n_0 as smallint_n0," +
        "   smallint_array.arr_n_1 as smallint_n1," +
        "   decimal_array.arr_n_0 as decimal_n0," +
        "   decimal_array.arr_n_1 as decimal_n1," +
        "   boolean_array.arr_n_0 as boolean_n0," +
        "   boolean_array.arr_n_1 as boolean_n1," +
        "   bigint_array.arr_n_0 as bigint_n0," +
        "   bigint_array.arr_n_1 as bigint_n1," +
        "   float_array.arr_n_0 as float_n0," +
        "   float_array.arr_n_1 as float_n1," +
        "   double_array.arr_n_0 as double_n0," +
        "   double_array.arr_n_1 as double_n1," +
        "   date_array.arr_n_0 as date_n0," +
        "   date_array.arr_n_1 as date_n1," +
        "   timestamp_array.arr_n_0 as timestamp_n0," +
        "   timestamp_array.arr_n_1 as timestamp_n1 " +
        "FROM " +
        "   int_array," +
        "   string_array," +
        "   varchar_array," +
        "   char_array," +
        "   tinyint_array," +
        "   smallint_array," +
        "   decimal_array," +
        "   boolean_array," +
        "   bigint_array," +
        "   float_array," +
        "   double_array," +
        "   date_array," +
        "   timestamp_array " +
        "WHERE " +
        "   int_array.rid=string_array.rid AND" +
        "   int_array.rid=varchar_array.rid AND" +
        "   int_array.rid=char_array.rid AND" +
        "   int_array.rid=tinyint_array.rid AND" +
        "   int_array.rid=smallint_array.rid AND" +
        "   int_array.rid=decimal_array.rid AND" +
        "   int_array.rid=boolean_array.rid AND" +
        "   int_array.rid=bigint_array.rid AND" +
        "   int_array.rid=float_array.rid AND" +
        "   int_array.rid=double_array.rid AND" +
        "   int_array.rid=date_array.rid AND" +
        "   int_array.rid=timestamp_array.rid "
    );
  }

  @Test
  public void intArray() throws Exception {

    // Nesting 0: reading ARRAY<INT>
    testBuilder()
        .sqlQuery("SELECT arr_n_0 FROM hive.`int_array`")
        .unOrdered()
        .baselineColumns("arr_n_0")
        .baselineValuesForSingleColumn(asList(-1, 0, 1))
        .baselineValuesForSingleColumn(emptyList())
        .baselineValuesForSingleColumn(Collections.singletonList(100500))
        .go();

    // Nesting 1: reading ARRAY<ARRAY<INT>>
    testBuilder()
        .sqlQuery("SELECT arr_n_1 FROM hive.`int_array`")
        .unOrdered()
        .baselineColumns("arr_n_1")
        .baselineValuesForSingleColumn(asList(asList(-1, 0, 1), asList(-2, 1)))
        .baselineValuesForSingleColumn(asList(emptyList(), emptyList()))
        .baselineValuesForSingleColumn(asList(asList(100500, 500100)))
        .go();

    // Nesting 2: reading ARRAY<ARRAY<ARRAY<INT>>>
    testBuilder()
        .sqlQuery("SELECT arr_n_2 FROM hive.`int_array` order by rid")
        .ordered()
        .baselineColumns("arr_n_2")
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(7, 81),//[0][0]
                    asList(-92, 54, -83),//[0][1]
                    asList(-10, -59)//[0][2]
                ),
                asList( // [1]
                    asList(-43, -80)//[1][0]
                ),
                asList( // [2]
                    asList(-70, -62)//[2][0]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(34, -18)//[0][0]
                ),
                asList( // [1]
                    asList(-87, 87),//[1][0]
                    asList(52, 58),//[1][1]
                    asList(58, 20, -81),//[1][2]
                    asList(-94, -93)//[1][3]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(-56, 9),//[0][0]
                    asList(39, 5)//[0][1]
                ),
                asList( // [1]
                    asList(28, 88, -28)//[1][0]
                )
            )
        ).go();
  }

  @Test
  public void intArrayInJoin() throws Exception {
    testBuilder()
        .sqlQuery("SELECT a.rid as gid, a.arr_n_0 as an0, b.arr_n_0 as bn0 " +
            "FROM hive.int_array a " +
            "INNER JOIN hive.int_array b " +
            "ON a.rid=b.rid WHERE a.rid=1")
        .unOrdered()
        .baselineColumns("gid", "an0", "bn0")
        .baselineValues(1, asList(-1, 0, 1), asList(-1, 0, 1))
        .go();
    testBuilder()
        .sqlQuery("SELECT * FROM (SELECT a.rid as gid, a.arr_n_0 as an0, b.arr_n_0 as bn0,c.arr_n_0 as cn0 " +
            "FROM hive.int_array a,hive.int_array b, hive.int_array c " +
            "WHERE a.rid=b.rid AND a.rid=c.rid) WHERE gid=1")
        .unOrdered()
        .baselineColumns("gid", "an0", "bn0", "cn0")
        .baselineValues(1, asList(-1, 0, 1), asList(-1, 0, 1), asList(-1, 0, 1))
        .go();
  }

  @Test
  public void intArrayByIndex() throws Exception {
    // arr_n_0 array<int>, arr_n_1 array<array<int>>
    testBuilder()
        .sqlQuery("SELECT arr_n_0[0], arr_n_0[1], arr_n_1[0], arr_n_1[1], arr_n_0[3], arr_n_1[3] FROM hive.`int_array`")
        .unOrdered()
        .baselineColumns("EXPR$0", "EXPR$1", "EXPR$2", "EXPR$3", "EXPR$4", "EXPR$5")
        .baselineValues(-1, 0, asList(-1, 0, 1), asList(-2, 1), null, emptyList())
        .baselineValues(null, null, emptyList(), emptyList(), null, emptyList())
        .baselineValues(100500, null, asList(100500, 500100), emptyList(), null, emptyList())
        .go();
  }

  @Test
  public void intArrayFlatten() throws Exception {
    // arr_n_0 array<int>, arr_n_1 array<array<int>>
    testBuilder()
        .sqlQuery("SELECT rid, FLATTEN(arr_n_0) FROM hive.`int_array`")
        .unOrdered()
        .baselineColumns("rid", "EXPR$1")
        .baselineValues(1, -1)
        .baselineValues(1, 0)
        .baselineValues(1, 1)
        .baselineValues(3, 100500)
        .go();

    testBuilder()
        .sqlQuery("SELECT rid, FLATTEN(arr_n_1) FROM hive.`int_array`")
        .unOrdered()
        .baselineColumns("rid", "EXPR$1")
        .baselineValues(1, asList(-1, 0, 1))
        .baselineValues(1, asList(-2, 1))
        .baselineValues(2, emptyList())
        .baselineValues(2, emptyList())
        .baselineValues(3, asList(100500, 500100))
        .go();

    testBuilder()
        .sqlQuery("SELECT rid, FLATTEN(FLATTEN(arr_n_1)) FROM hive.`int_array`")
        .unOrdered()
        .baselineColumns("rid", "EXPR$1")
        .baselineValues(1, -1)
        .baselineValues(1, 0)
        .baselineValues(1, 1)
        .baselineValues(1, -2)
        .baselineValues(1, 1)
        .baselineValues(3, 100500)
        .baselineValues(3, 500100)
        .go();
  }

  @Test
  public void intArrayRepeatedCount() throws Exception {
    testBuilder()
        .sqlQuery("SELECT rid, REPEATED_COUNT(arr_n_0), REPEATED_COUNT(arr_n_1) FROM hive.`int_array`")
        .unOrdered()
        .baselineColumns("rid", "EXPR$1", "EXPR$2")
        .baselineValues(1, 3, 2)
        .baselineValues(2, 0, 2)
        .baselineValues(3, 1, 1)
        .go();
  }

  @Test
  public void intArrayRepeatedContains() throws Exception {
    testBuilder()
        .sqlQuery("SELECT rid FROM hive.`int_array` WHERE REPEATED_CONTAINS(arr_n_0, 100500)")
        .unOrdered()
        .baselineColumns("rid")
        .baselineValues(3)
        .go();
  }

  @Test
  public void intArrayDescribe() throws Exception {
    testBuilder()
        .sqlQuery("DESCRIBE hive.`int_array` arr_n_0")
        .unOrdered()
        .baselineColumns("COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE")
        .baselineValues("arr_n_0", "ARRAY", "YES") //todo: fix to ARRAY<INTEGER>
        .go();
    testBuilder()
        .sqlQuery("DESCRIBE hive.`int_array` arr_n_1")
        .unOrdered()
        .baselineColumns("COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE")
        .baselineValues("arr_n_1", "ARRAY", "YES") // todo: ARRAY<ARRAY<INTEGER>>
        .go();
  }

  @Test
  public void intArrayTypeOfKindFunctions() throws Exception {
    testBuilder()
        .sqlQuery("select " +
            "sqlTypeOf(arr_n_0), sqlTypeOf(arr_n_1),  " +
            "typeOf(arr_n_0), typeOf(arr_n_1), " +
            "modeOf(arr_n_0), modeOf(arr_n_1), " +
            "drillTypeOf(arr_n_0), drillTypeOf(arr_n_1) " +
            "from hive.`int_array` limit 1")
        .unOrdered()
        .baselineColumns(
            "EXPR$0", "EXPR$1",
            "EXPR$2", "EXPR$3",
            "EXPR$4", "EXPR$5",
            "EXPR$6", "EXPR$7"
        )
        .baselineValues(
            "ARRAY", "ARRAY", // why not ARRAY<INTEGER> | ARRAY<ARRAY<INTEGER>> ?
            "INT", "LIST",    // todo: is it ok ?
            "ARRAY", "ARRAY",
            "INT", "LIST"    // todo: is it ok ?
        )
        .go();
  }

  @Test
  public void stringArray() throws Exception {
    // Nesting 0: reading ARRAY<STRING>
    testBuilder()
        .sqlQuery("SELECT arr_n_0 FROM hive.`string_array`")
        .unOrdered()
        .baselineColumns("arr_n_0")
        .baselineValuesForSingleColumn(asList(new Text("First Value Of Array"), new Text("komlnp"), new Text("The Last Value")))
        .baselineValuesForSingleColumn(emptyList())
        .baselineValuesForSingleColumn(Collections.singletonList(new Text("ABCaBcA-1-2-3")))
        .go();

    // Nesting 1: reading ARRAY<ARRAY<STRING>>
    testBuilder()
        .sqlQuery("SELECT arr_n_1 FROM hive.`string_array`")
        .unOrdered()
        .baselineColumns("arr_n_1")
        .baselineValuesForSingleColumn(asList(asList(new Text("Array 0, Value 0"), new Text("Array 0, Value 1")), asList(new Text("Array 1"))))
        .baselineValuesForSingleColumn(asList(emptyList(), emptyList()))
        .baselineValuesForSingleColumn(asList(asList(new Text("One"))))
        .go();

    // Nesting 2: reading ARRAY<ARRAY<ARRAY<STRING>>>
    testBuilder()
        .sqlQuery("SELECT arr_n_2 FROM hive.`string_array` order by rid")
        .ordered()
        .baselineColumns("arr_n_2")
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(new Text("dhMGOr1QVO"), new Text("NZpzBl"), new Text("LC8mjYyOJ7l8dHUpk"))//[0][0]
                ),
                asList( // [1]
                    asList(new Text("JH")),//[1][0]
                    asList(new Text("aVxgfxAu")),//[1][1]
                    asList(new Text("fF amN8z8"))//[1][2]
                ),
                asList( // [2]
                    asList(new Text("denwte5R39dSb2PeG"), new Text("Gbosj97RXTvBK1w"), new Text("S3whFvN")),//[2][0]
                    asList(new Text("2sNbYGQhkt303Gnu"), new Text("rwG"), new Text("SQH766A8XwHg2pTA6a"))//[2][1]
                ),
                asList( // [3]
                    asList(new Text("L"), new Text("khGFDtDluFNoo5hT")),//[3][0]
                    asList(new Text("b8")),//[3][1]
                    asList(new Text("Z"))//[3][2]
                ),
                asList( // [4]
                    asList(new Text("DTEuW"), new Text("b0Wt84hIl"), new Text("A1H")),//[4][0]
                    asList(new Text("h2zXh3Qc"), new Text("NOcgU8"), new Text("RGfVgv2rvDG")),//[4][1]
                    asList(new Text("Hfn1ov9hB7fZN"), new Text("0ZgCD3"))//[4][2]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(new Text("nk"), new Text("HA"), new Text("CgAZCxTbTrFWJL3yM")),//[0][0]
                    asList(new Text("T7fGXYwtBb"), new Text("G6vc")),//[0][1]
                    asList(new Text("GrwB5j3LBy9")),//[0][2]
                    asList(new Text("g7UreegD1H97"), new Text("dniQ5Ehhps7c1pBuM"), new Text("S wSNMGj7c")),//[0][3]
                    asList(new Text("iWTEJS0"), new Text("4F"))//[0][4]
                ),
                asList( // [1]
                    asList(new Text("YpRcC01u6i6KO"), new Text("ujpMrvEfUWfKm"), new Text("2d")),//[1][0]
                    asList(new Text("2"), new Text("HVDH"), new Text("5Qx Q6W112"))//[1][1]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(new Text("S8d2vjNu680hSim6iJ")),//[0][0]
                    asList(new Text("lRLaT9RvvgzhZ3C"), new Text("igSX1CP"), new Text("FFZMwMvAOod8")),//[0][1]
                    asList(new Text("iBX"), new Text("sG")),//[0][2]
                    asList(new Text("ChRjuDPz99WeU9"), new Text("2gBBmMUXV9E5E"), new Text(" VkEARI2upO"))//[0][3]
                ),
                asList( // [1]
                    asList(new Text("UgMok3Q5wmd")),//[1][0]
                    asList(new Text("8Zf9CLfUSWK"), new Text(""), new Text("NZ7v")),//[1][1]
                    asList(new Text("vQE3I5t26"), new Text("251BeQJue"))//[1][2]
                ),
                asList( // [2]
                    asList(new Text("Rpo8"))//[2][0]
                ),
                asList( // [3]
                    asList(new Text("jj3njyupewOM Ej0pu"), new Text("aePLtGgtyu4aJ5"), new Text("cKHSvNbImH1MkQmw0Cs")),//[3][0]
                    asList(new Text("VSO5JgI2x7TnK31L5"), new Text("hIub"), new Text("eoBSa0zUFlwroSucU")),//[3][1]
                    asList(new Text("V8Gny91lT"), new Text("5hBncDZ"))//[3][2]
                ),
                asList( // [4]
                    asList(new Text("Y3"), new Text("StcgywfU"), new Text("BFTDChc")),//[4][0]
                    asList(new Text("5JNwXc2UHLld7"), new Text("v")),//[4][1]
                    asList(new Text("9UwBhJMSDftPKuGC")),//[4][2]
                    asList(new Text("E hQ9NJkc0GcMlB"), new Text("IVND1Xp1Nnw26DrL9"))//[4][3]
                )
            )
        ).go();
  }

  @Test
  public void stringArrayByIndex() throws Exception {
    // arr_n_0 array<string>, arr_n_1 array<array<string>>
    testBuilder()
        .sqlQuery("SELECT arr_n_0[0], arr_n_0[1], arr_n_1[0], arr_n_1[1], arr_n_0[3], arr_n_1[3] FROM hive.`string_array`")
        .unOrdered()
        .baselineColumns(
            "EXPR$0",
            "EXPR$1",
            "EXPR$2",
            "EXPR$3",
            "EXPR$4",
            "EXPR$5")
        .baselineValues(
            "First Value Of Array",
            "komlnp",
            asList(new Text("Array 0, Value 0"), new Text("Array 0, Value 1")),
            asList(new Text("Array 1")),
            null,
            emptyList()
        )
        .baselineValues(
            null,
            null,
            emptyList(),
            emptyList(),
            null,
            emptyList())
        .baselineValues(
            "ABCaBcA-1-2-3",
            null,
            asList(new Text("One")),
            emptyList(),
            null,
            emptyList())
        .go();
  }

  @Test
  public void varcharArray() throws Exception {
    // Nesting 0: reading ARRAY<VARCHAR(5)>
    testBuilder()
        .sqlQuery("SELECT arr_n_0 FROM hive.`varchar_array`")
        .unOrdered()
        .baselineColumns("arr_n_0")
        .baselineValuesForSingleColumn(asList(new Text("Five"), new Text("One"), new Text("T")))
        .baselineValuesForSingleColumn(emptyList())
        .baselineValuesForSingleColumn(asList(new Text("ZZ0"), new Text("-c54g"), new Text("ooo"), new Text("k22k")))
        .go();

    // Nesting 1: reading ARRAY<ARRAY<VARCHAR(5)>>
    testBuilder()
        .sqlQuery("SELECT arr_n_1 FROM hive.`varchar_array`")
        .unOrdered()
        .baselineColumns("arr_n_1")
        .baselineValuesForSingleColumn(asList(
            asList(new Text("Five"), new Text("One"), new Text("$42")),
            asList(new Text("T"), new Text("K"), new Text("O"))
        ))
        .baselineValuesForSingleColumn(asList(emptyList(), emptyList()))
        .baselineValuesForSingleColumn(asList(asList(new Text("-c54g"))))
        .go();

    // Nesting 2: reading ARRAY<ARRAY<ARRAY<VARCHAR(5)>>>
    testBuilder()
        .sqlQuery("SELECT arr_n_2 FROM hive.`varchar_array` order by rid")
        .ordered()
        .baselineColumns("arr_n_2")
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(new Text("")),//[0][0]
                    asList(new Text("Gt"), new Text(""), new Text("")),//[0][1]
                    asList(new Text("9R3y")),//[0][2]
                    asList(new Text("X3a4"))//[0][3]
                ),
                asList( // [1]
                    asList(new Text("o"), new Text("6T"), new Text("QKAZ")),//[1][0]
                    asList(new Text(""), new Text("xf8r"), new Text("As")),//[1][1]
                    asList(new Text("5kS3"))//[1][2]
                ),
                asList( // [2]
                    asList(new Text(""), new Text("S7Gx")),//[2][0]
                    asList(new Text("ml"), new Text("27pL"), new Text("VPxr")),//[2][1]
                    asList(new Text("")),//[2][2]
                    asList(new Text("e"), new Text("Dj"))//[2][3]
                ),
                asList( // [3]
                    asList(new Text(""), new Text("XYO"), new Text("fEWz")),//[3][0]
                    asList(new Text(""), new Text("oU")),//[3][1]
                    asList(new Text("o 8"), new Text(""), new Text("")),//[3][2]
                    asList(new Text("giML"), new Text("H7g")),//[3][3]
                    asList(new Text("SWX9"), new Text("H"), new Text("emwt"))//[3][4]
                ),
                asList( // [4]
                    asList(new Text("Sp"))//[4][0]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(new Text("GCx")),//[0][0]
                    asList(new Text(""), new Text("V")),//[0][1]
                    asList(new Text("pF"), new Text("R7"), new Text("")),//[0][2]
                    asList(new Text(""), new Text("AKal"))//[0][3]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(new Text("m"), new Text("MBAv"), new Text("7R9F")),//[0][0]
                    asList(new Text("ovv")),//[0][1]
                    asList(new Text("p 7l"))//[0][2]
                )
            )
        )
        .go();

  }

  @Test
  public void varcharArrayByIndex() throws Exception {
    // arr_n_0 array<varchar>, arr_n_1 array<array<varchar>>
    testBuilder()
        .sqlQuery("SELECT arr_n_0[0], arr_n_0[1], arr_n_1[0], arr_n_1[1], arr_n_0[3], arr_n_1[3] FROM hive.`varchar_array`")
        .unOrdered()
        .baselineColumns("EXPR$0", "EXPR$1", "EXPR$2", "EXPR$3", "EXPR$4", "EXPR$5")
        .baselineValues(
            "Five",
            "One",
            asList(new Text("Five"), new Text("One"), new Text("$42")),
            asList(new Text("T"), new Text("K"), new Text("O")),
            null,
            emptyList())
        .baselineValues(
            null,
            null,
            emptyList(),
            emptyList(),
            null,
            emptyList())
        .baselineValues(
            "ZZ0",
            "-c54g",
            asList(new Text("-c54g")),
            emptyList(),
            "k22k",
            emptyList())
        .go();
  }

  @Test
  public void charArray() throws Exception {
    // Nesting 0: reading ARRAY<CHAR(2)>
    testBuilder()
        .sqlQuery("SELECT arr_n_0 FROM hive.`char_array`")
        .unOrdered()
        .baselineColumns("arr_n_0")
        .baselineValuesForSingleColumn(asList(new Text("aa"), new Text("cc"), new Text("ot")))
        .baselineValuesForSingleColumn(emptyList())
        .baselineValuesForSingleColumn(asList(new Text("+a"), new Text("-c"), new Text("*t")))
        .go();

    // Nesting 1: reading ARRAY<ARRAY<CHAR(2)>>
    testBuilder()
        .sqlQuery("SELECT arr_n_1 FROM hive.`char_array`")
        .unOrdered()
        .baselineColumns("arr_n_1")
        .baselineValuesForSingleColumn(asList(
            asList(new Text("aa")),
            asList(new Text("cc"), new Text("ot"))))
        .baselineValuesForSingleColumn(asList(emptyList(), emptyList()))
        .baselineValuesForSingleColumn(asList(asList(new Text("*t"))))
        .go();

    // Nesting 2: reading ARRAY<ARRAY<ARRAY<CHAR(2)>>>
    testBuilder()
        .sqlQuery("SELECT arr_n_2 FROM hive.`char_array` order by rid")
        .ordered()
        .baselineColumns("arr_n_2")
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(new Text("eT"))//[0][0]
                ),
                asList( // [1]
                    asList(new Text("w9"), new Text("fC"), new Text("ww")),//[1][0]
                    asList(new Text("3o"), new Text("f7"), new Text("Za")),//[1][1]
                    asList(new Text("lX"), new Text("iv"), new Text("jI"))//[1][2]
                ),
                asList( // [2]
                    asList(new Text("S3"), new Text("Qa"), new Text("aG")),//[2][0]
                    asList(new Text("bj"), new Text("gc"), new Text("NO"))//[2][1]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(new Text("PV"), new Text("tH"), new Text("B7")),//[0][0]
                    asList(new Text("uL")),//[0][1]
                    asList(new Text("7b"), new Text("uf")),//[0][2]
                    asList(new Text("zj")),//[0][3]
                    asList(new Text("sA"), new Text("hf"), new Text("hR"))//[0][4]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(new Text("W1"), new Text("FS")),//[0][0]
                    asList(new Text("le"), new Text("c0")),//[0][1]
                    asList(new Text(""), new Text("0v"))//[0][2]
                ),
                asList( // [1]
                    asList(new Text("gj"))//[1][0]
                )
            )
        )
        .go();
  }

  @Test
  public void charArrayByIndex() throws Exception {
    // arr_n_0 array<char>, arr_n_1 array<array<char>>
    testBuilder()
        .sqlQuery("SELECT arr_n_0[0], arr_n_0[1], arr_n_1[0], arr_n_1[1], arr_n_0[3], arr_n_1[3] FROM hive.`char_array`")
        .unOrdered()
        .baselineColumns("EXPR$0", "EXPR$1", "EXPR$2", "EXPR$3", "EXPR$4", "EXPR$5")
        .baselineValues(
            "aa",
            "cc",
            asList(new Text("aa")),
            asList(new Text("cc"), new Text("ot")),
            null,
            emptyList())
        .baselineValues(
            null,
            null,
            emptyList(),
            emptyList(),
            null,
            emptyList())
        .baselineValues(
            "+a",
            "-c",
            asList(new Text("*t")),
            emptyList(),
            null,
            emptyList())
        .go();
  }

  @Test
  public void tinyintArray() throws Exception {
    // Nesting 0: reading ARRAY<TINYINT>
    testBuilder()
        .sqlQuery("SELECT arr_n_0 FROM hive.`tinyint_array`")
        .unOrdered()
        .baselineColumns("arr_n_0")
        .baselineValuesForSingleColumn(asList(-128, 0, 127))
        .baselineValuesForSingleColumn(emptyList())
        .baselineValuesForSingleColumn(asList(-101))
        .go();

    // Nesting 1: reading ARRAY<ARRAY<TINYINT>>
    testBuilder()
        .sqlQuery("SELECT arr_n_1 FROM hive.`tinyint_array`")
        .unOrdered()
        .baselineColumns("arr_n_1")
        .baselineValuesForSingleColumn(asList(asList(-128, -127), asList(0, 1), asList(127, 126)))
        .baselineValuesForSingleColumn(asList(emptyList(), emptyList()))
        .baselineValuesForSingleColumn(asList(asList(-102)))
        .go();

    // Nesting 2: reading ARRAY<ARRAY<ARRAY<TINYINT>>>
    testBuilder()
        .sqlQuery("SELECT arr_n_2 FROM hive.`tinyint_array` order by rid")
        .ordered()
        .baselineColumns("arr_n_2")
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(31, 65, 54),//[0][0]
                    asList(66),//[0][1]
                    asList(22),//[0][2]
                    asList(-33, -125, 116)//[0][3]
                ),
                asList( // [1]
                    asList(-5, -10)//[1][0]
                ),
                asList( // [2]
                    asList(78),//[2][0]
                    asList(86),//[2][1]
                    asList(90, 34),//[2][2]
                    asList(32)//[2][3]
                ),
                asList( // [3]
                    asList(103, -49, -33),//[3][0]
                    asList(-30),//[3][1]
                    asList(107, 24, 74),//[3][2]
                    asList(16, -58)//[3][3]
                ),
                asList( // [4]
                    asList(-119, -8),//[4][0]
                    asList(50, -99, 26),//[4][1]
                    asList(-119)//[4][2]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(-90, -113),//[0][0]
                    asList(71, -65)//[0][1]
                ),
                asList( // [1]
                    asList(88, -83)//[1][0]
                ),
                asList( // [2]
                    asList(11),//[2][0]
                    asList(121, -57)//[2][1]
                ),
                asList( // [3]
                    asList(-79),//[3][0]
                    asList(16, -111, -111),//[3][1]
                    asList(90, 106),//[3][2]
                    asList(33, 29, 42),//[3][3]
                    asList(74)//[3][4]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(74, -115),//[0][0]
                    asList(19, 85, 3)//[0][1]
                )
            )
        )
        .go();
  }

  @Test
  public void tinyintArrayByIndex() throws Exception {
    // arr_n_0 array<tinyint>, arr_n_1 array<array<tinyint>>
    testBuilder()
        .sqlQuery("SELECT arr_n_0[0], arr_n_0[1], arr_n_1[0], arr_n_1[1], arr_n_0[3], arr_n_1[3] FROM hive.`tinyint_array`")
        .unOrdered()
        .baselineColumns("EXPR$0", "EXPR$1", "EXPR$2", "EXPR$3", "EXPR$4", "EXPR$5")
        .baselineValues(-128, 0, asList(-128, -127), asList(0, 1), null, emptyList())
        .baselineValues(null, null, emptyList(), emptyList(), null, emptyList())
        .baselineValues(-101, null, asList(-102), emptyList(), null, emptyList())
        .go();
  }

  @Test
  public void smallintArray() throws Exception {
    // Nesting 0: reading ARRAY<SMALLINT>
    testBuilder()
        .sqlQuery("SELECT arr_n_0 FROM hive.`smallint_array`")
        .unOrdered()
        .baselineColumns("arr_n_0")
        .baselineValuesForSingleColumn(asList(-32768, 0, 32767))
        .baselineValuesForSingleColumn(emptyList())
        .baselineValuesForSingleColumn(asList(10500))
        .go();

    // Nesting 1: reading ARRAY<ARRAY<SMALLINT>>
    testBuilder()
        .sqlQuery("SELECT arr_n_1 FROM hive.`smallint_array`")
        .unOrdered()
        .baselineColumns("arr_n_1")
        .baselineValuesForSingleColumn(asList(asList(-32768, -32768), asList(0, 0), asList(32767, 32767)))
        .baselineValuesForSingleColumn(asList(emptyList(), emptyList()))
        .baselineValuesForSingleColumn(asList(asList(10500, 5010)))
        .go();

    // Nesting 2: reading ARRAY<ARRAY<ARRAY<SMALLINT>>>
    testBuilder()
        .sqlQuery("SELECT arr_n_2 FROM hive.`smallint_array` order by rid")
        .ordered()
        .baselineColumns("arr_n_2")
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(-28752)//[0][0]
                ),
                asList( // [1]
                    asList(17243, 15652),//[1][0]
                    asList(-9684),//[1][1]
                    asList(10176, 18123),//[1][2]
                    asList(-15404, 15420),//[1][3]
                    asList(11136, -19435)//[1][4]
                ),
                asList( // [2]
                    asList(-29634, -12695),//[2][0]
                    asList(4350, -24289, -10889)//[2][1]
                ),
                asList( // [3]
                    asList(13731),//[3][0]
                    asList(27661, -15794, 21784),//[3][1]
                    asList(14341, -4635),//[3][2]
                    asList(1601, -29973),//[3][3]
                    asList(2750, 30373, -11630)//[3][4]
                ),
                asList( // [4]
                    asList(-11383)//[4][0]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(23860),//[0][0]
                    asList(-27345, 19068),//[0][1]
                    asList(-7174, 286, 14673)//[0][2]
                ),
                asList( // [1]
                    asList(14844, -9087),//[1][0]
                    asList(-25185, 219),//[1][1]
                    asList(26875),//[1][2]
                    asList(-4699),//[1][3]
                    asList(-3853, -15729, 11472)//[1][4]
                ),
                asList( // [2]
                    asList(-29142),//[2][0]
                    asList(-13859),//[2][1]
                    asList(-23073, 31368, -26542)//[2][2]
                ),
                asList( // [3]
                    asList(14914, 14656),//[3][0]
                    asList(4636, 6289)//[3][1]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(10426, 31865),//[0][0]
                    asList(-19088),//[0][1]
                    asList(-4774),//[0][2]
                    asList(17988)//[0][3]
                ),
                asList( // [1]
                    asList(-6214, -26836, 30715)//[1][0]
                ),
                asList( // [2]
                    asList(-4231),//[2][0]
                    asList(31742, -661),//[2][1]
                    asList(-22842, 4203),//[2][2]
                    asList(18278)//[2][3]
                )
            )
        )
        .go();
  }

  @Test
  public void decimalArray() throws Exception {
    // Nesting 0: reading ARRAY<DECIMAL(9,3)>
    testBuilder()
        .sqlQuery("SELECT arr_n_0 FROM hive.`decimal_array`")
        .unOrdered()
        .baselineColumns("arr_n_0")
        .baselineValuesForSingleColumn(asList(new BigDecimal("-100000.000"), new BigDecimal("102030.001"), new BigDecimal("0.001")))
        .baselineValuesForSingleColumn(emptyList())
        .baselineValuesForSingleColumn(Collections.singletonList(new BigDecimal("-10.500")))
        .go();

    // Nesting 1: reading ARRAY<ARRAY<DECIMAL(9,3)>>
    testBuilder()
        .sqlQuery("SELECT arr_n_1 FROM hive.`decimal_array`")
        .unOrdered()
        .baselineColumns("arr_n_1")
        .baselineValuesForSingleColumn(asList(
            asList(new BigDecimal("-100000.000"), new BigDecimal("102030.001")),
            asList(new BigDecimal("0.101"), new BigDecimal("0.102")),
            asList(new BigDecimal("0.001"), new BigDecimal("327670.001"))))
        .baselineValuesForSingleColumn(asList(emptyList(), emptyList()))
        .baselineValuesForSingleColumn(asList(asList(new BigDecimal("10.500"), new BigDecimal("5.010"))))
        .go();

    // Nesting 2: reading ARRAY<ARRAY<ARRAY<DECIMAL(9,3)>>>
    testBuilder()
        .sqlQuery("SELECT arr_n_2 FROM hive.`decimal_array` order by rid")
        .ordered()
        .baselineColumns("arr_n_2")
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(new BigDecimal("9.453")),//[0][0]
                    asList(new BigDecimal("8.233"), new BigDecimal("-146577.465")),//[0][1]
                    asList(new BigDecimal("-911144.423"), new BigDecimal("-862766.866"), new BigDecimal("-129948.784"))//[0][2]
                ),
                asList( // [1]
                    asList(new BigDecimal("931346.867"))//[1][0]
                ),
                asList( // [2]
                    asList(new BigDecimal("81.750")),//[2][0]
                    asList(new BigDecimal("587225.077"), new BigDecimal("-3.930")),//[2][1]
                    asList(new BigDecimal("0.042")),//[2][2]
                    asList(new BigDecimal("-342346.511"))//[2][3]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(new BigDecimal("375098.406"), new BigDecimal("84.509")),//[0][0]
                    asList(new BigDecimal("-446325.287"), new BigDecimal("3.671")),//[0][1]
                    asList(new BigDecimal("286958.380"), new BigDecimal("314821.890"), new BigDecimal("18513.303")),//[0][2]
                    asList(new BigDecimal("-444023.971"), new BigDecimal("827746.528"), new BigDecimal("-54.986")),//[0][3]
                    asList(new BigDecimal("-44520.406"))//[0][4]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(new BigDecimal("906668.849"), new BigDecimal("1.406")),//[0][0]
                    asList(new BigDecimal("-494177.333"), new BigDecimal("952997.058"))//[0][1]
                ),
                asList( // [1]
                    asList(new BigDecimal("642385.159"), new BigDecimal("369753.830"), new BigDecimal("634889.981")),//[1][0]
                    asList(new BigDecimal("83970.515"), new BigDecimal("-847315.758"), new BigDecimal("-0.600")),//[1][1]
                    asList(new BigDecimal("73013.870")),//[1][2]
                    asList(new BigDecimal("337872.675"), new BigDecimal("375940.114"), new BigDecimal("-2.670")),//[1][3]
                    asList(new BigDecimal("-7.899"), new BigDecimal("755611.538"))//[1][4]
                )
            )
        )
        .go();
  }

  @Test
  public void booleanArray() throws Exception {
    // Nesting 0: reading ARRAY<BOOLEAN>
    testBuilder()
        .sqlQuery("SELECT arr_n_0 FROM hive.`boolean_array`")
        .unOrdered()
        .baselineColumns("arr_n_0")
        .baselineValuesForSingleColumn(asList(false, true, false, true, false))
        .baselineValuesForSingleColumn(emptyList())
        .baselineValuesForSingleColumn(Collections.singletonList(true))
        .go();

    // Nesting 1: reading ARRAY<ARRAY<BOOLEAN>>
    testBuilder()
        .sqlQuery("SELECT arr_n_1 FROM hive.`boolean_array`")
        .unOrdered()
        .baselineColumns("arr_n_1")
        .baselineValuesForSingleColumn(asList(asList(true, false, true), asList(false, false)))
        .baselineValuesForSingleColumn(asList(emptyList(), emptyList()))
        .baselineValuesForSingleColumn(asList(asList(false, true)))
        .go();

    // Nesting 2: reading ARRAY<ARRAY<ARRAY<BOOLEAN>>>
    testBuilder()
        .sqlQuery("SELECT arr_n_2 FROM hive.`boolean_array` order by rid")
        .ordered()
        .baselineColumns("arr_n_2")
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(false, true)//[0][0]
                ),
                asList( // [1]
                    asList(true),//[1][0]
                    asList(false, true),//[1][1]
                    asList(true),//[1][2]
                    asList(true)//[1][3]
                ),
                asList( // [2]
                    asList(false),//[2][0]
                    asList(true, false, false),//[2][1]
                    asList(true, true),//[2][2]
                    asList(false, true, false)//[2][3]
                ),
                asList( // [3]
                    asList(false, true),//[3][0]
                    asList(true, false),//[3][1]
                    asList(true, false, true)//[3][2]
                ),
                asList( // [4]
                    asList(false),//[4][0]
                    asList(false),//[4][1]
                    asList(false)//[4][2]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(false, true),//[0][0]
                    asList(false),//[0][1]
                    asList(false, false),//[0][2]
                    asList(true, true, true),//[0][3]
                    asList(false)//[0][4]
                ),
                asList( // [1]
                    asList(false, false, true)//[1][0]
                ),
                asList( // [2]
                    asList(false, true),//[2][0]
                    asList(true, false)//[2][1]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(true, true),//[0][0]
                    asList(false, true, false),//[0][1]
                    asList(true),//[0][2]
                    asList(true, true, false)//[0][3]
                ),
                asList( // [1]
                    asList(false),//[1][0]
                    asList(false, true),//[1][1]
                    asList(false),//[1][2]
                    asList(false)//[1][3]
                ),
                asList( // [2]
                    asList(true, true, true),//[2][0]
                    asList(true, true, true),//[2][1]
                    asList(false),//[2][2]
                    asList(false)//[2][3]
                ),
                asList( // [3]
                    asList(false, false)//[3][0]
                )
            )
        )
        .go();
  }

  @Test
  public void bigintArray() throws Exception {
    // Nesting 0: reading ARRAY<BIGINT>
    testBuilder()
        .sqlQuery("SELECT arr_n_0 FROM hive.`bigint_array`")
        .unOrdered()
        .baselineColumns("arr_n_0")
        .baselineValuesForSingleColumn(asList(-9223372036854775808L, 0L, 10000000010L, 9223372036854775807L))
        .baselineValuesForSingleColumn(emptyList())
        .baselineValuesForSingleColumn(Collections.singletonList(10005000L))
        .go();

    // Nesting 1: reading ARRAY<ARRAY<BIGINT>>
    testBuilder()
        .sqlQuery("SELECT arr_n_1 FROM hive.`bigint_array`")
        .unOrdered()
        .baselineColumns("arr_n_1")
        .baselineValuesForSingleColumn(asList(
            asList(-9223372036854775808L, 0L, 10000000010L),
            asList(9223372036854775807L, 9223372036854775807L)))
        .baselineValuesForSingleColumn(asList(emptyList(), emptyList()))
        .baselineValuesForSingleColumn(asList(asList(10005000L, 100050010L)))
        .go();

    // Nesting 2: reading ARRAY<ARRAY<ARRAY<BIGINT>>>
    testBuilder()
        .sqlQuery("SELECT arr_n_2 FROM hive.`bigint_array` order by rid")
        .ordered()
        .baselineColumns("arr_n_2")
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(7345032157033769004L),//[0][0]
                    asList(-2306607274383855051L, 3656249581579032003L)//[0][1]
                ),
                asList( // [1]
                    asList(6044100897358387146L, 4737705104728607904L)//[1][0]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(4833583793282587107L, -8917877693351417844L, -3226305034926780974L)//[0][0]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(8679405200896733338L, 8581721713860760451L, 1150622751848016114L),//[0][0]
                    asList(-6672104994192826124L, 4807952216371616134L),//[0][1]
                    asList(-7874492057876324257L)//[0][2]
                ),
                asList( // [1]
                    asList(8197656735200560038L),//[1][0]
                    asList(7643173300425098029L, -3186442699228156213L, -8370345321491335247L),//[1][1]
                    asList(8781633305391982544L, -7187468334864189662L)//[1][2]
                ),
                asList( // [2]
                    asList(6685428436181310098L),//[2][0]
                    asList(1358587806266610826L),//[2][1]
                    asList(-2077124879355227614L, -6787493227661516341L),//[2][2]
                    asList(3713296190482954025L, -3890396613053404789L),//[2][3]
                    asList(4636761050236625699L, 5268453104977816600L)//[2][4]
                )
            )
        )
        .go();
  }

  @Test
  public void floatArray() throws Exception {
    // Nesting 0: reading ARRAY<FLOAT>
    testBuilder()
        .sqlQuery("SELECT arr_n_0 FROM hive.`float_array`")
        .unOrdered()
        .baselineColumns("arr_n_0")
        .baselineValuesForSingleColumn(asList(-32.058f, 94.47389f, 16.107912f))
        .baselineValuesForSingleColumn(emptyList())
        .baselineValuesForSingleColumn(Collections.singletonList(25.96484f))
        .go();

    // Nesting 1: reading ARRAY<ARRAY<FLOAT>>
    testBuilder()
        .sqlQuery("SELECT arr_n_1 FROM hive.`float_array`")
        .unOrdered()
        .baselineColumns("arr_n_1")
        .baselineValuesForSingleColumn(asList(asList(-82.399826f, 12.633938f, 86.19402f), asList(-13.03544f, 64.65487f)))
        .baselineValuesForSingleColumn(asList(emptyList(), emptyList()))
        .baselineValuesForSingleColumn(asList(asList(15.259451f, -15.259451f)))
        .go();

    // Nesting 2: reading ARRAY<ARRAY<ARRAY<FLOAT>>>
    testBuilder()
        .sqlQuery("SELECT arr_n_2 FROM hive.`float_array` order by rid")
        .ordered()
        .baselineColumns("arr_n_2")
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(-5.6506114f),//[0][0]
                    asList(26.546333f, 3724.8389f),//[0][1]
                    asList(-53.65775f, 686.8335f, -0.99032f)//[0][2]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(29.042528f),//[0][0]
                    asList(3524.3398f, -8856.58f, 6.8508215f)//[0][1]
                ),
                asList( // [1]
                    asList(-0.73994386f, -2.0008986f),//[1][0]
                    asList(-9.903006f, -271.26172f),//[1][1]
                    asList(-131.80347f),//[1][2]
                    asList(39.721367f, -4870.5444f),//[1][3]
                    asList(-1.4830998f, -766.3066f, -0.1659732f)//[1][4]
                ),
                asList( // [2]
                    asList(3467.0298f, -240.64255f),//[2][0]
                    asList(2.4072556f, -85.89145f)//[2][1]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(-888.68243f, -38.09065f),//[0][0]
                    asList(-6948.154f, -185.64319f, 0.7401936f),//[0][1]
                    asList(-705.2718f, -932.4041f)//[0][2]
                ),
                asList( // [1]
                    asList(-2.581712f, 0.28686252f, -0.98652786f),//[1][0]
                    asList(-57.448563f, -0.0057083773f, -0.21712556f),//[1][1]
                    asList(-8.076653f, -8149.519f, -7.5968184f),//[1][2]
                    asList(8.823492f),//[1][3]
                    asList(-9134.323f, 467.53275f, -59.763447f)//[1][4]
                ),
                asList( // [2]
                    asList(0.33596575f, 6805.2256f, -3087.9531f),//[2][0]
                    asList(9816.865f, -164.90712f, -1.9071647f)//[2][1]
                ),
                asList( // [3]
                    asList(-0.23883149f),//[3][0]
                    asList(-5.3763375f, -4.7661624f)//[3][1]
                ),
                asList( // [4]
                    asList(-52.42167f, 247.91452f),//[4][0]
                    asList(9499.771f),//[4][1]
                    asList(-0.6549191f, 4340.83f)//[4][2]
                )
            )
        )
        .go();
  }

  @Test
  public void doubleArray() throws Exception {
    // Nesting 0: reading ARRAY<DOUBLE>
    testBuilder()
        .sqlQuery("SELECT arr_n_0 FROM hive.`double_array`")
        .unOrdered()
        .baselineColumns("arr_n_0")
        .baselineValuesForSingleColumn(asList(-13.241563769628, 0.3436367772981237, 9.73366))
        .baselineValuesForSingleColumn(emptyList())
        .baselineValuesForSingleColumn(asList(15.581409176959358))
        .go();

    // Nesting 1: reading ARRAY<ARRAY<DOUBLE>>
    testBuilder()
        .sqlQuery("SELECT arr_n_1 FROM hive.`double_array`")
        .unOrdered()
        .baselineColumns("arr_n_1")
        .baselineValuesForSingleColumn(asList(asList(-24.049666910012498, 14.975034200, 1.19975056092457), asList(-2.293376758961259, 80.783)))
        .baselineValuesForSingleColumn(asList(emptyList(), emptyList()))
        .baselineValuesForSingleColumn(asList(asList(0.47745359256854, -0.47745359256854)))
        .go();

    // Nesting 2: reading ARRAY<ARRAY<ARRAY<DOUBLE>>>
    testBuilder()
        .sqlQuery("SELECT arr_n_2 FROM hive.`double_array` order by rid")
        .ordered()
        .baselineColumns("arr_n_2")
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(-9.269519394436928),//[0][0]
                    asList(0.7319990286742192, 55.53357952933713, -4.450389221972496)//[0][1]
                ),
                asList( // [1]
                    asList(0.8453724066773386)//[1][0]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(-7966.1700155142025, 2519.664646202656),//[0][0]
                    asList(-0.4584683555041169),//[0][1]
                    asList(-860.4673046946417, 6.371900064750405, 0.4722917366204724)//[0][2]
                ),
                asList( // [1]
                    asList(-62.76596817199298),//[1][0]
                    asList(712.7880069076203, -5.14172156610055),//[1][1]
                    asList(3891.128276893486, -0.5008908018575201)//[1][2]
                ),
                asList( // [2]
                    asList(246.42074787345825, -0.7252828610111548),//[2][0]
                    asList(-845.6633966327038, -436.5267842528363)//[2][1]
                ),
                asList( // [3]
                    asList(5.177407969462521),//[3][0]
                    asList(0.10545048230228471, 0.7364424942282094),//[3][1]
                    asList(-373.3798205258425, -79.65616885610245)//[3][2]
                ),
                asList( // [4]
                    asList(-744.3464669962211, 3.8376055596419754),//[4][0]
                    asList(5784.252615154324, -4792.10612059247, -2535.4093308546435)//[4][1]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(0.054727088545119096, 0.3289046600776335, -183.0613955159468)//[0][0]
                ),
                asList( // [1]
                    asList(-1653.1119499932845, 5132.117249049659),//[1][0]
                    asList(735.8474815185632, -5.4205625353286795),//[1][1]
                    asList(2.9513430741605107, -7513.09536433704),//[1][2]
                    asList(1660.4238619967039),//[1][3]
                    asList(472.7475322920831)//[1][4]
                )
            )
        )
        .go();
  }

  @Test
  public void dateArray() throws Exception {

    // Nesting 0: reading ARRAY<DATE>
    testBuilder()
        .sqlQuery("SELECT arr_n_0 FROM hive.`date_array`")
        .unOrdered()
        .baselineColumns("arr_n_0")
        .baselineValuesForSingleColumn(asList(
            parseLocalDate("2018-10-21"),
            parseLocalDate("2017-07-11"),
            parseLocalDate("2018-09-23")))
        .baselineValuesForSingleColumn(emptyList())
        .baselineValuesForSingleColumn(asList(parseLocalDate("2018-07-14")))
        .go();

    // Nesting 1: reading ARRAY<ARRAY<DATE>>
    testBuilder()
        .sqlQuery("SELECT arr_n_1 FROM hive.`date_array`")
        .unOrdered()
        .baselineColumns("arr_n_1")
        .baselineValuesForSingleColumn(asList(
            asList(parseLocalDate("2017-03-21"), parseLocalDate("2017-09-10"), parseLocalDate("2018-01-17")),
            asList(parseLocalDate("2017-03-24"), parseLocalDate("2018-09-22"))))
        .baselineValuesForSingleColumn(asList(emptyList(), emptyList()))
        .baselineValuesForSingleColumn(asList(asList(parseLocalDate("2017-08-09"), parseLocalDate("2017-08-28"))))
        .go();

    // Nesting 2: reading ARRAY<ARRAY<ARRAY<DATE>>>
    testBuilder()
        .sqlQuery("SELECT arr_n_2 FROM hive.`date_array` order by rid")
        .ordered()
        .baselineColumns("arr_n_2")
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(parseLocalDate("1952-08-24")),//[0][0]
                    asList(parseLocalDate("1968-10-05"), parseLocalDate("1951-07-27")),//[0][1]
                    asList(parseLocalDate("1943-11-18"), parseLocalDate("1991-04-27"))//[0][2]
                ),
                asList( // [1]
                    asList(parseLocalDate("1981-12-27"), parseLocalDate("1984-02-03")),//[1][0]
                    asList(parseLocalDate("1953-04-15"), parseLocalDate("2002-08-15"), parseLocalDate("1926-12-10")),//[1][1]
                    asList(parseLocalDate("2009-08-09"), parseLocalDate("1919-08-30"), parseLocalDate("1906-04-10")),//[1][2]
                    asList(parseLocalDate("1995-10-28"), parseLocalDate("1989-09-07")),//[1][3]
                    asList(parseLocalDate("2002-01-03"), parseLocalDate("1929-03-17"), parseLocalDate("1939-10-23"))//[1][4]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(parseLocalDate("1936-05-05"), parseLocalDate("1941-04-12"), parseLocalDate("1914-04-15"))//[0][0]
                ),
                asList( // [1]
                    asList(parseLocalDate("1944-05-09"), parseLocalDate("2002-02-11"))//[1][0]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(parseLocalDate("1965-04-18"), parseLocalDate("2012-11-07"), parseLocalDate("1961-03-15")),//[0][0]
                    asList(parseLocalDate("1922-05-22"), parseLocalDate("1978-03-25")),//[0][1]
                    asList(parseLocalDate("1935-05-29"))//[0][2]
                ),
                asList( // [1]
                    asList(parseLocalDate("1904-07-08"), parseLocalDate("1968-05-23"), parseLocalDate("1946-03-31")),//[1][0]
                    asList(parseLocalDate("2014-01-28")),//[1][1]
                    asList(parseLocalDate("1938-09-20"), parseLocalDate("1920-07-09"), parseLocalDate("1990-12-31")),//[1][2]
                    asList(parseLocalDate("1984-07-20"), parseLocalDate("1988-11-25")),//[1][3]
                    asList(parseLocalDate("1941-12-21"), parseLocalDate("1939-01-16"), parseLocalDate("2012-09-19"))//[1][4]
                ),
                asList( // [2]
                    asList(parseLocalDate("2020-12-28")),//[2][0]
                    asList(parseLocalDate("1930-11-13")),//[2][1]
                    asList(parseLocalDate("2014-05-02"), parseLocalDate("1935-02-16"), parseLocalDate("1919-01-17")),//[2][2]
                    asList(parseLocalDate("1972-04-20"), parseLocalDate("1951-05-30"), parseLocalDate("1963-01-11"))//[2][3]
                ),
                asList( // [3]
                    asList(parseLocalDate("1993-03-20"), parseLocalDate("1978-12-31")),//[3][0]
                    asList(parseLocalDate("1965-12-15"), parseLocalDate("1970-09-02"), parseLocalDate("2010-05-25"))//[3][1]
                )
            )
        )
        .go();
  }

  @Test
  public void timestampArray() throws Exception {
    // Nesting 0: reading ARRAY<TIMESTAMP>
    testBuilder()
        .sqlQuery("SELECT arr_n_0 FROM hive.`timestamp_array`")
        .unOrdered()
        .baselineColumns("arr_n_0")
        .baselineValuesForSingleColumn(asList(
            parseBest("2018-10-21 04:51:36"),
            parseBest("2017-07-11 09:26:48"),
            parseBest("2018-09-23 03:02:33")))
        .baselineValuesForSingleColumn(emptyList())
        .baselineValuesForSingleColumn(asList(parseBest("2018-07-14 05:20:34")))
        .go();

    // Nesting 1: reading ARRAY<ARRAY<TIMESTAMP>>
    testBuilder()
        .sqlQuery("SELECT arr_n_1 FROM hive.`timestamp_array`")
        .unOrdered()
        .baselineColumns("arr_n_1")
        .baselineValuesForSingleColumn(asList(
            asList(parseBest("2017-03-21 12:52:33"), parseBest("2017-09-10 01:29:24"), parseBest("2018-01-17 04:45:23")),
            asList(parseBest("2017-03-24 01:03:23"), parseBest("2018-09-22 05:00:26"))))
        .baselineValuesForSingleColumn(asList(emptyList(), emptyList()))
        .baselineValuesForSingleColumn(asList(asList(parseBest("2017-08-09 08:26:08"), parseBest("2017-08-28 09:47:23"))))
        .go();

    // Nesting 2: reading ARRAY<ARRAY<ARRAY<TIMESTAMP>>>
    testBuilder()
        .sqlQuery("SELECT arr_n_2 FROM hive.`timestamp_array` order by rid")
        .ordered()
        .baselineColumns("arr_n_2")
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(parseBest("1929-01-08 19:31:47")),//[0][0]
                    asList(parseBest("1968-07-02 15:13:55"), parseBest("1990-01-25 21:05:51"), parseBest("1950-10-26 19:16:10")),//[0][1]
                    asList(parseBest("1946-09-03 03:03:50"), parseBest("1987-03-29 11:27:05")),//[0][2]
                    asList(parseBest("1979-11-29 09:01:14"))//[0][3]
                ),
                asList( // [1]
                    asList(parseBest("2010-08-26 12:08:51"), parseBest("2012-02-05 02:34:22")),//[1][0]
                    asList(parseBest("1955-02-24 19:45:33")),//[1][1]
                    asList(parseBest("1994-06-19 09:33:56"), parseBest("1971-11-05 06:27:55"), parseBest("1925-04-11 13:55:48")),//[1][2]
                    asList(parseBest("1916-10-02 05:09:18"), parseBest("1995-04-11 18:05:51"), parseBest("1973-11-17 06:06:53"))//[1][3]
                ),
                asList( // [2]
                    asList(parseBest("1929-12-19 16:49:08"), parseBest("1942-10-28 04:55:13"), parseBest("1936-12-01 13:01:37")),//[2][0]
                    asList(parseBest("1926-12-09 07:34:14"), parseBest("1971-07-23 15:01:00"), parseBest("2014-01-07 06:29:03")),//[2][1]
                    asList(parseBest("2012-08-25 23:26:10")),//[2][2]
                    asList(parseBest("2010-03-04 08:31:54"), parseBest("1950-07-20 19:26:08"), parseBest("1953-03-16 16:13:24"))//[2][3]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(parseBest("1904-12-10 00:39:14")),//[0][0]
                    asList(parseBest("1994-04-12 23:06:07")),//[0][1]
                    asList(parseBest("1954-07-05 23:48:09"), parseBest("1913-03-03 18:47:14"), parseBest("1960-04-30 22:35:28")),//[0][2]
                    asList(parseBest("1962-09-26 17:11:12"), parseBest("1906-06-18 04:05:21"), parseBest("2003-06-19 05:15:24"))//[0][3]
                ),
                asList( // [1]
                    asList(parseBest("1929-03-20 06:33:40"), parseBest("1939-02-12 07:03:07"), parseBest("1945-02-16 21:18:16"))//[1][0]
                ),
                asList( // [2]
                    asList(parseBest("1969-08-11 22:25:31"), parseBest("1944-08-11 02:57:58")),//[2][0]
                    asList(parseBest("1989-03-18 13:33:56"), parseBest("1961-06-06 04:44:50"))//[2][1]
                )
            )
        )
        .baselineValuesForSingleColumn(
            asList( // row
                asList( // [0]
                    asList(parseBest("1999-12-07 01:16:45")),//[0][0]
                    asList(parseBest("1903-12-11 04:28:20"), parseBest("2007-01-03 19:27:28")),//[0][1]
                    asList(parseBest("2018-03-16 15:43:19"), parseBest("2002-09-16 08:58:40"), parseBest("1956-05-16 17:47:44")),//[0][2]
                    asList(parseBest("2006-09-19 18:38:19"), parseBest("2016-01-21 12:39:30"))//[0][3]
                )
            )
        )
        .go();
  }

  @Test
  public void binaryArray() throws Exception {
    // Nesting 0: reading ARRAY<BINARY>
    testBuilder()
        .sqlQuery("SELECT arr_n_0 FROM hive.`binary_array`")
        .unOrdered()
        .baselineColumns("arr_n_0")
        .baselineValuesForSingleColumn(asList(new StringBytes("First"), new StringBytes("Second"), new StringBytes("Third")))
        .baselineValuesForSingleColumn(asList(new StringBytes("First")))
        .go();
  }

  @Test
  public void arrayViewDefinedInHive() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM hive.`arr_view` WHERE vwrid=1")
        .unOrdered()
        .baselineColumns("vwrid", "int_n0", "int_n1", "string_n0", "string_n1",
            "varchar_n0", "varchar_n1", "char_n0", "char_n1", "tinyint_n0",
            "tinyint_n1", "smallint_n0", "smallint_n1", "decimal_n0", "decimal_n1",
            "boolean_n0", "boolean_n1", "bigint_n0", "bigint_n1", "float_n0", "float_n1",
            "double_n0", "double_n1", "date_n0", "date_n1", "timestamp_n0", "timestamp_n1")
        .baselineValues(
            1,

            asList(-1, 0, 1),
            asList(asList(-1, 0, 1), asList(-2, 1)),

            asList(new Text("First Value Of Array"), new Text("komlnp"), new Text("The Last Value")),
            asList(asList(new Text("Array 0, Value 0"), new Text("Array 0, Value 1")), asList(new Text("Array 1"))),

            asList(new Text("Five"), new Text("One"), new Text("T")),
            asList(asList(new Text("Five"), new Text("One"), new Text("$42")), asList(new Text("T"), new Text("K"), new Text("O"))),

            asList(new Text("aa"), new Text("cc"), new Text("ot")),
            asList(asList(new Text("aa")), asList(new Text("cc"), new Text("ot"))),

            asList(-128, 0, 127),
            asList(asList(-128, -127), asList(0, 1), asList(127, 126)),

            asList(-32768, 0, 32767),
            asList(asList(-32768, -32768), asList(0, 0), asList(32767, 32767)),

            asList(new BigDecimal("-100000.000"), new BigDecimal("102030.001"), new BigDecimal("0.001")),
            asList(asList(new BigDecimal("-100000.000"), new BigDecimal("102030.001")), asList(new BigDecimal("0.101"), new BigDecimal("0.102")),
                asList(new BigDecimal("0.001"), new BigDecimal("327670.001"))),

            asList(false, true, false, true, false),
            asList(asList(true, false, true), asList(false, false)),

            asList(-9223372036854775808L, 0L, 10000000010L, 9223372036854775807L),
            asList(asList(-9223372036854775808L, 0L, 10000000010L), asList(9223372036854775807L, 9223372036854775807L)),

            asList(-32.058f, 94.47389f, 16.107912f),
            asList(asList(-82.399826f, 12.633938f, 86.19402f), asList(-13.03544f, 64.65487f)),

            asList(-13.241563769628, 0.3436367772981237, 9.73366),
            asList(asList(-24.049666910012498, 14.975034200, 1.19975056092457), asList(-2.293376758961259, 80.783)),

            asList(parseLocalDate("2018-10-21"), parseLocalDate("2017-07-11"), parseLocalDate("2018-09-23")),
            asList(asList(parseLocalDate("2017-03-21"), parseLocalDate("2017-09-10"), parseLocalDate("2018-01-17")),
                asList(parseLocalDate("2017-03-24"), parseLocalDate("2018-09-22"))),

            asList(parseBest("2018-10-21 04:51:36"), parseBest("2017-07-11 09:26:48"), parseBest("2018-09-23 03:02:33")),
            asList(asList(parseBest("2017-03-21 12:52:33"), parseBest("2017-09-10 01:29:24"), parseBest("2018-01-17 04:45:23")),
                asList(parseBest("2017-03-24 01:03:23"), parseBest("2018-09-22 05:00:26")))
        )
        .go();
  }

  @Test
  public void arrayViewDefinedInDrill() throws Exception {
    queryBuilder().sql(
        "CREATE VIEW " + StoragePluginTestUtils.DFS_TMP_SCHEMA + ".`dfs_arr_vw` AS " +
            "SELECT " +
            "   t1.rid as vwrid," +
            "   t1.arr_n_0 as int_n0," +
            "   t1.arr_n_1 as int_n1," +
            "   t2.arr_n_0 as string_n0," +
            "   t2.arr_n_1 as string_n1," +
            "   t3.arr_n_0 as varchar_n0," +
            "   t3.arr_n_1 as varchar_n1," +
            "   t4.arr_n_0 as char_n0," +
            "   t4.arr_n_1 as char_n1," +
            "   t5.arr_n_0 as tinyint_n0," +
            "   t5.arr_n_1 as tinyint_n1," +
            "   t6.arr_n_0 as smallint_n0," +
            "   t6.arr_n_1 as smallint_n1," +
            "   t7.arr_n_0 as decimal_n0," +
            "   t7.arr_n_1 as decimal_n1," +
            "   t8.arr_n_0 as boolean_n0," +
            "   t8.arr_n_1 as boolean_n1," +
            "   t9.arr_n_0 as bigint_n0," +
            "   t9.arr_n_1 as bigint_n1," +
            "   t10.arr_n_0 as float_n0," +
            "   t10.arr_n_1 as float_n1," +
            "   t11.arr_n_0 as double_n0," +
            "   t11.arr_n_1 as double_n1," +
            "   t12.arr_n_0 as date_n0," +
            "   t12.arr_n_1 as date_n1," +
            "   t13.arr_n_0 as timestamp_n0," +
            "   t13.arr_n_1 as timestamp_n1 " +
            "FROM " +
            "   hive.int_array t1," +
            "   hive.string_array t2," +
            "   hive.varchar_array t3," +
            "   hive.char_array t4," +
            "   hive.tinyint_array t5," +
            "   hive.smallint_array t6," +
            "   hive.decimal_array t7," +
            "   hive.boolean_array t8," +
            "   hive.bigint_array t9," +
            "   hive.float_array t10," +
            "   hive.double_array t11," +
            "   hive.date_array t12," +
            "   hive.timestamp_array t13 " +
            "WHERE " +
            "   t1.rid=t2.rid AND" +
            "   t1.rid=t3.rid AND" +
            "   t1.rid=t4.rid AND" +
            "   t1.rid=t5.rid AND" +
            "   t1.rid=t6.rid AND" +
            "   t1.rid=t7.rid AND" +
            "   t1.rid=t8.rid AND" +
            "   t1.rid=t9.rid AND" +
            "   t1.rid=t10.rid AND" +
            "   t1.rid=t11.rid AND" +
            "   t1.rid=t12.rid AND" +
            "   t1.rid=t13.rid "
    ).run();

    testBuilder()
        .sqlQuery("SELECT * FROM " + StoragePluginTestUtils.DFS_TMP_SCHEMA + ".`dfs_arr_vw` WHERE vwrid=1")
        .unOrdered()
        .baselineColumns("vwrid", "int_n0", "int_n1", "string_n0", "string_n1",
            "varchar_n0", "varchar_n1", "char_n0", "char_n1", "tinyint_n0",
            "tinyint_n1", "smallint_n0", "smallint_n1", "decimal_n0", "decimal_n1",
            "boolean_n0", "boolean_n1", "bigint_n0", "bigint_n1", "float_n0", "float_n1",
            "double_n0", "double_n1", "date_n0", "date_n1", "timestamp_n0", "timestamp_n1")
        .baselineValues(
            1,

            asList(-1, 0, 1),
            asList(asList(-1, 0, 1), asList(-2, 1)),

            asList(new Text("First Value Of Array"), new Text("komlnp"), new Text("The Last Value")),
            asList(asList(new Text("Array 0, Value 0"), new Text("Array 0, Value 1")), asList(new Text("Array 1"))),

            asList(new Text("Five"), new Text("One"), new Text("T")),
            asList(asList(new Text("Five"), new Text("One"), new Text("$42")), asList(new Text("T"), new Text("K"), new Text("O"))),

            asList(new Text("aa"), new Text("cc"), new Text("ot")),
            asList(asList(new Text("aa")), asList(new Text("cc"), new Text("ot"))),

            asList(-128, 0, 127),
            asList(asList(-128, -127), asList(0, 1), asList(127, 126)),

            asList(-32768, 0, 32767),
            asList(asList(-32768, -32768), asList(0, 0), asList(32767, 32767)),

            asList(new BigDecimal("-100000.000"), new BigDecimal("102030.001"), new BigDecimal("0.001")),
            asList(asList(new BigDecimal("-100000.000"), new BigDecimal("102030.001")), asList(new BigDecimal("0.101"), new BigDecimal("0.102")),
                asList(new BigDecimal("0.001"), new BigDecimal("327670.001"))),

            asList(false, true, false, true, false),
            asList(asList(true, false, true), asList(false, false)),

            asList(-9223372036854775808L, 0L, 10000000010L, 9223372036854775807L),
            asList(asList(-9223372036854775808L, 0L, 10000000010L), asList(9223372036854775807L, 9223372036854775807L)),

            asList(-32.058f, 94.47389f, 16.107912f),
            asList(asList(-82.399826f, 12.633938f, 86.19402f), asList(-13.03544f, 64.65487f)),

            asList(-13.241563769628, 0.3436367772981237, 9.73366),
            asList(asList(-24.049666910012498, 14.975034200, 1.19975056092457), asList(-2.293376758961259, 80.783)),

            asList(parseLocalDate("2018-10-21"), parseLocalDate("2017-07-11"), parseLocalDate("2018-09-23")),
            asList(asList(parseLocalDate("2017-03-21"), parseLocalDate("2017-09-10"), parseLocalDate("2018-01-17")),
                asList(parseLocalDate("2017-03-24"), parseLocalDate("2018-09-22"))),

            asList(parseBest("2018-10-21 04:51:36"), parseBest("2017-07-11 09:26:48"), parseBest("2018-09-23 03:02:33")),
            asList(asList(parseBest("2017-03-21 12:52:33"), parseBest("2017-09-10 01:29:24"), parseBest("2018-01-17 04:45:23")),
                asList(parseBest("2017-03-24 01:03:23"), parseBest("2018-09-22 05:00:26")))
        )
        .go();
  }

  /**
   * Workaround {@link StringBytes#equals(Object)} implementation
   * used to compare binary array elements.
   * See {@link TestHiveArrays#binaryArray()} for sample usage.
   */
  private static final class StringBytes {

    private final byte[] bytes;

    private StringBytes(String s) {
      bytes = s.getBytes();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof byte[]) {
        return Arrays.equals(bytes, (byte[]) obj);
      }
      return (obj == this) || (obj instanceof StringBytes
          && Arrays.equals(bytes, ((StringBytes) obj).bytes));
    }

    @Override
    public String toString() {
      return new String(bytes);
    }

  }

}
