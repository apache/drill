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

package org.apache.drill.exec.store.msaccess;

import org.apache.drill.categories.RowSetTest;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryTestUtil;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import static org.junit.Assert.assertEquals;

@Category(RowSetTest.class)
public class TestMSAccessReader extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
  }

  @Test
  public void testStarQuery() throws Exception {
    String sql = "SELECT * FROM table(cp.`data/V2019/extDateTestV2019.accdb` (type=> 'msaccess', tableName => 'Table1')) LIMIT 5";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("ID", MinorType.INT)
        .addNullable("Field1", MinorType.VARCHAR)
        .addNullable("DateExt", MinorType.TIMESTAMP)
        .addNullable("DateNormal", MinorType.TIMESTAMP)
        .addNullable("DateExtStr", MinorType.VARCHAR)
        .addNullable("DateNormalCalc", MinorType.TIMESTAMP)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(1, "row1", QueryTestUtil.ConvertDateToLong("2020-06-17T00:00:00Z"), QueryTestUtil.ConvertDateToLong("2020-06-17T00:00:00Z"), "6/17/2020", QueryTestUtil.ConvertDateToLong("2020-06-17T00:00:00Z"))
        .addRow(2, "row2", QueryTestUtil.ConvertDateToLong("2021-06-14T00:00:00Z"), QueryTestUtil.ConvertDateToLong("2021-06-14T00:00:00Z"), "6/14/2021", QueryTestUtil.ConvertDateToLong("2021-06-14T00:00:00Z"))
        .addRow(3, "row3", QueryTestUtil.ConvertDateToLong("2021-06-14T12:45:00Z"), QueryTestUtil.ConvertDateToLong("2021-06-14T12:45:00Z"), "6/14/2021 12:45:00.0000000 PM", QueryTestUtil.ConvertDateToLong("2021-06-14T12:45:00Z"))
        .addRow(4, "row4", QueryTestUtil.ConvertDateToLong("2021-06-14T01:45:00Z"), QueryTestUtil.ConvertDateToLong("2021-06-14T01:45:00Z"), "6/14/2021 1:45:00.0000000 AM", QueryTestUtil.ConvertDateToLong("2021-06-14T01:45:00Z"))
        .addRow(5, "row5", null, null, null, null)
        .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testExplicitQuery() throws Exception {
    String sql = "SELECT ID, Field1, DateExt, DateNormal, DateExtStr, DateNormalCalc " +
        "FROM table(cp.`data/V2019/extDateTestV2019.accdb` (type=> 'msaccess', tableName => 'Table1')) LIMIT 5";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("ID", MinorType.INT)
        .addNullable("Field1", MinorType.VARCHAR)
        .addNullable("DateExt", MinorType.TIMESTAMP)
        .addNullable("DateNormal", MinorType.TIMESTAMP)
        .addNullable("DateExtStr", MinorType.VARCHAR)
        .addNullable("DateNormalCalc", MinorType.TIMESTAMP)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(1, "row1", QueryTestUtil.ConvertDateToLong("2020-06-17T00:00:00Z"), QueryTestUtil.ConvertDateToLong("2020-06-17T00:00:00Z"), "6/17/2020", QueryTestUtil.ConvertDateToLong("2020-06-17T00:00:00Z"))
        .addRow(2, "row2", QueryTestUtil.ConvertDateToLong("2021-06-14T00:00:00Z"), QueryTestUtil.ConvertDateToLong("2021-06-14T00:00:00Z"), "6/14/2021", QueryTestUtil.ConvertDateToLong("2021-06-14T00:00:00Z"))
        .addRow(3, "row3", QueryTestUtil.ConvertDateToLong("2021-06-14T12:45:00Z"), QueryTestUtil.ConvertDateToLong("2021-06-14T12:45:00Z"), "6/14/2021 12:45:00.0000000 PM", QueryTestUtil.ConvertDateToLong("2021-06-14T12:45:00Z"))
        .addRow(4, "row4", QueryTestUtil.ConvertDateToLong("2021-06-14T01:45:00Z"), QueryTestUtil.ConvertDateToLong("2021-06-14T01:45:00Z"), "6/14/2021 1:45:00.0000000 AM", QueryTestUtil.ConvertDateToLong("2021-06-14T01:45:00Z"))
        .addRow(5, "row5", null, null, null, null)
        .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testStarQueryWithDataTypes() throws Exception {
    String sql = "SELECT * " +
        "FROM table(cp.`data/V2010/testV2010.accdb` (type=> 'msaccess', tableName => 'Table1')) LIMIT 5";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("A", MinorType.VARCHAR)
        .addNullable("B", MinorType.VARCHAR)
        .addNullable("C", MinorType.TINYINT)
        .addNullable("D", MinorType.SMALLINT)
        .addNullable("E", MinorType.INT)
        .addNullable("F", MinorType.FLOAT8)
        .addNullable("G", MinorType.TIMESTAMP)
        .addNullable("H", MinorType.VARDECIMAL)
        .addNullable("I", MinorType.BIT)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("a", "b", 0, 0, 0, 0.0, QueryTestUtil.ConvertDateToLong("1981-12-12T00:00:00Z"), 0, false)
        .addRow("abcdefg", "hijklmnop", 2, 222, 333333333, 444.555, QueryTestUtil.ConvertDateToLong("1974-09-21T00:00:00Z"), 4, true)
        .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testMetadataStarQuery() throws Exception {
    String sql = "SELECT * FROM cp.`data/V2019/extDateTestV2019.accdb`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("table", MinorType.VARCHAR)
        .add("created_date", MinorType.TIMESTAMP)
        .add("updated_date", MinorType.TIMESTAMP)
        .add("row_count", MinorType.INT)
        .add("col_count", MinorType.INT)
        .addArray("columns", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("Table1", QueryTestUtil.ConvertDateToLong("2021-06-03T20:09:56.993Z"),
            QueryTestUtil.ConvertDateToLong("2021-06-03T20:09:56.993Z"), 9, 6, strArray("ID", "Field1", "DateExt", "DateNormal", "DateExtStr", "DateNormalCalc"))
        .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }


  @Test
  public void testSerDe() throws Exception {
    String sql = "SELECT COUNT(*) AS cnt FROM " +
        "table(cp.`data/V2019/extDateTestV2019.accdb` (type=> 'msaccess', tableName => 'Table1'))";
    String plan = queryBuilder().sql(sql).explainJson();
    long cnt = queryBuilder().physical(plan).singletonLong();
    assertEquals("Counts should match",9L, cnt);
  }
}
