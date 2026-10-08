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

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import org.apache.drill.categories.RowSetTest;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryTestUtil;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


import java.io.File;

import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(RowSetTest.class)
public class TestMSAccessReader extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
  }

  /**
   * Writes an Access file whose only table is linked to a database that does not exist. The target is
   * a path under the test directory so that nothing outside the sandbox (e.g. an SMB share) is touched
   * if the link were ever followed.
   */
  static File writeLinkedTableFile(String name, String linkedDbPath) throws Exception {
    File mdb = new File(dirTestWatcher.getRootDir(), name);
    try (Database db = DatabaseBuilder.create(Database.FileFormat.V2003, mdb)) {
      db.createLinkedTable("Linked", linkedDbPath, "Table1");
    }
    return mdb;
  }

  private static File writeLinkedTableFile(String name) throws Exception {
    return writeLinkedTableFile(name, new File(dirTestWatcher.getRootDir(), "missing_target.mdb").getAbsolutePath());
  }

  @Test
  public void testLinkedTableIsNotResolved() throws Exception {
    // A malicious file can point a linked table at any local path or UNC share; reading it must
    // not make the Drillbit open that path.
    File mdb = writeLinkedTableFile("linked.mdb");
    String sql = "SELECT * FROM table(dfs.`" + mdb.getName() + "` (type=> 'msaccess', tableName => 'Linked'))";
    try {
      client.queryBuilder().sql(sql).run();
      fail("Expected the linked database to be refused");
    } catch (UserRemoteException e) {
      assertEquals(DrillPBError.ErrorType.PERMISSION, e.getErrorType());
      assertTrue(e.getMessage(), e.getMessage().contains("Table Linked is a linked table"));
      assertTrue(e.getMessage(), e.getMessage().contains(MSAccessFormatPlugin.ALLOW_LINKED_DATABASES));
    }
  }

  @Test
  public void testLinkedTableCannotBeEnabledFromTableFunction() throws Exception {
    // Enabling linked tables is a boot option, so a query must not be able to switch it on.
    File mdb = writeLinkedTableFile("linked_table_function.mdb");
    String sql = "SELECT * FROM table(dfs.`" + mdb.getName() +
        "` (type=> 'msaccess', tableName => 'Linked', allowLinkedDatabases => true))";
    try {
      client.queryBuilder().sql(sql).run();
      fail("Expected the query to fail");
    } catch (UserRemoteException e) {
      assertNotEquals(e.getMessage(), DrillPBError.ErrorType.DATA_READ, e.getErrorType());
      assertFalse(e.getMessage(), e.getMessage().contains("does not exist"));
    }
  }

  @Test
  public void testMetadataQueryListsLinkedTable() throws Exception {
    File mdb = writeLinkedTableFile("linked_metadata.mdb");
    String sql = "SELECT `table`, row_count, col_count FROM dfs.`" + mdb.getName() + "`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("table", MinorType.VARCHAR)
        .addNullable("row_count", MinorType.INT)
        .addNullable("col_count", MinorType.INT)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("Linked", null, null)
        .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
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
        .addNullable("created_date", MinorType.TIMESTAMP)
        .addNullable("updated_date", MinorType.TIMESTAMP)
        .addNullable("row_count", MinorType.INT)
        .addNullable("col_count", MinorType.INT)
        .addArray("columns", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("Table1", QueryTestUtil.ConvertDateToLong("2021-06-03T20:09:56.993Z"),
            QueryTestUtil.ConvertDateToLong("2021-06-03T20:25:26.483Z"), 9, 6, strArray("ID", "Field1", "DateExt", "DateNormal", "DateExtStr", "DateNormalCalc"))
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
