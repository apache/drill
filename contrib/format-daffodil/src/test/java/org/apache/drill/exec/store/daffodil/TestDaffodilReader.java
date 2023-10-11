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

package org.apache.drill.exec.store.daffodil;

import org.apache.drill.categories.RowSetTest;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Category(RowSetTest.class)
public class TestDaffodilReader extends ClusterTest {

  String schemaURIRoot = "file:///opt/drill/contrib/format-daffodil/src/test/resources/";
  @BeforeClass
  public static void setup() throws Exception {
    // boilerplate call to start test rig
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));

    DaffodilFormatConfig formatConfig =
        new DaffodilFormatConfig(null,
            "",
            "",
            "",
            false);

    cluster.defineFormat("dfs", "daffodil", formatConfig);

    // Needed to test against compressed files.
    // Copies data from src/test/resources to the dfs root.
    dirTestWatcher.copyResourceToRoot(Paths.get("data/"));
    dirTestWatcher.copyResourceToRoot(Paths.get("schema/"));
  }

  private String selectRow(String schema, String file) {
    return "SELECT * FROM table(dfs.`data/" + file + "` " +
        " (type => 'daffodil'," +
        " validationMode => 'true', " +
        " schemaURI => '" + schemaURIRoot + "schema/" + schema + ".dfdl.xsd'," +
        " rootName => 'row'," +
        " rootNamespace => null " +
        "))";
  }

  /**
   * This unit test tests a simple data file
   *
   * @throws Exception Throw exception if anything goes wrong
   */
  @Test
  public void testSimpleQuery1() throws Exception {

    QueryBuilder qb = client.queryBuilder();
    QueryBuilder query = qb.sql(selectRow("simple", "data01Int.dat.gz"));
    RowSet results = query.rowSet();
    results.print();
    assertEquals(1, results.rowCount());

    // create the expected metadata and data for this test
    // metadata first
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("col", MinorType.INT)
        .buildSchema();

    RowSet expected = client.rowSetBuilder(expectedSchema)
        .addRow(0x00000101) // aka 257
        .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testSimpleQuery2() throws Exception {

    QueryBuilder qb = client.queryBuilder();
    QueryBuilder query = qb.sql(selectRow("simple","data06Int.dat"));
    RowSet results = query.rowSet();
    results.print();
    assertEquals(6, results.rowCount());

    // create the expected metadata and data for this test
    // metadata first
    TupleMetadata expectedSchema = new SchemaBuilder()
            .add("col", MinorType.INT)
            .buildSchema();

    RowSet expected = client.rowSetBuilder(expectedSchema)
            .addRow(0x00000101)
            .addRow(0x00000102)
            .addRow(0x00000103)
            .addRow(0x00000104)
            .addRow(0x00000105)
            .addRow(0x00000106)
            .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testComplexQuery1() throws Exception {

    QueryBuilder qb = client.queryBuilder();
    QueryBuilder query = qb.sql(selectRow("complex1", "data02Int.dat"));
    RowSet results = query.rowSet();
    results.print();
    assertEquals(1, results.rowCount());

    RowSetReader rdr = results.reader();
    rdr.next();
    String col = rdr.getAsString();
    assertEquals("{257, 258}", col);
    assertFalse(rdr.next());
    results.clear();
  }

  @Test
  public void testComplexQuery2() throws Exception {

    QueryBuilder qb = client.queryBuilder();
    QueryBuilder query = qb.sql(selectRow("complex1", "data06Int.dat"));
    RowSet results = query.rowSet();
    results.print();
    assertEquals(3, results.rowCount());

    RowSetReader rdr = results.reader();
    rdr.next();
    String map = rdr.getAsString();
    assertEquals("{257, 258}", map);
    rdr.next();
    map = rdr.getAsString();
    assertEquals("{259, 260}", map);
    rdr.next();
    map = rdr.getAsString();
    assertEquals("{261, 262}", map);
    assertFalse(rdr.next());
    results.clear();
  }

  /**
   * Tests data which is rows of two ints and an array containing a
   * map containing two ints.
   * Each row can be visualized like this: "{257, 258, [{259, 260},...]}"
   * @throws Exception
   */
  @Test
  public void testComplexArrayQuery1() throws Exception {

    QueryBuilder qb = client.queryBuilder();
    QueryBuilder query = qb.sql(selectRow("complexArray1", "data12Int.dat"));
    RowSet results = query.rowSet();
    results.print();
    assertEquals(1, results.rowCount());

    RowSetReader rdr = results.reader();
    rdr.next();
    String map = rdr.getAsString();
    assertEquals("{257, 258, [{259, 260}, {261, 262}, {257, 258}, {259, 260}, {261, 262}]}", map);
    assertFalse(rdr.next());
    results.clear();
  }

  /**
   * Tests data which is an array of ints in one column of the row set
   * @throws Exception
   */
  @Test
  public void testSimpleArrayQuery1() throws Exception {

    QueryBuilder qb = client.queryBuilder();
    QueryBuilder query = qb.sql(selectRow("simpleArrayField1", "data12Int.dat"));
    RowSet results = query.rowSet();
    results.print();
    assertEquals(1, results.rowCount());

    RowSetReader rdr = results.reader();
    rdr.next();
    String map = rdr.getAsString();
    assertEquals("{[257, 258, 259, 260, 261, 262, 257, 258, 259, 260, 261, 262]}", map);
    assertFalse(rdr.next());
    results.clear();
  }

  /**
   * Tests data which is rows of two ints and an array containing a
   * map containing an int and a vector of ints.
   * Each row can be visualized like this: "{257, 258, [{259, [260, 261, 262]},...]}"
   * @throws Exception
   */
  @Test
  public void testComplexArrayQuery2() throws Exception {

    QueryBuilder qb = client.queryBuilder();
    QueryBuilder query = qb.sql(selectRow("complexArray2", "data12Int.dat"));
    RowSet results = query.rowSet();
    results.print();
    assertEquals(1, results.rowCount());

    RowSetReader rdr = results.reader();
    rdr.next();
    String map = rdr.getAsString();
    assertEquals("{257, 258, [{259, [260, 261, 262]}, {257, [258, 259, 260]}, {261, [262]}]}", map);
    assertFalse(rdr.next());
    results.clear();
  }

  @Test
  public void testMoreTypes1() throws Exception {

    QueryBuilder qb = client.queryBuilder();
    QueryBuilder query = qb.sql(selectRow("moreTypes1", "moreTypes1.txt.dat"));
    RowSet results = query.rowSet();
    results.print();
    assertEquals(2, results.rowCount());

    RowSetReader rdr = results.reader();
    rdr.next();
    String map = rdr.getAsString();
    assertEquals("{2147483647, 9223372036854775807, 32767, 127, true, " +
        "1.7976931348623157E308, 3.4028235E38, [31, 32, 33, 34, 35, 36, 37, 38], \"daffodil\"}", map);
    rdr.next();
    map = rdr.getAsString();
    assertEquals("{-2147483648, -9223372036854775808, -32768, -128, false, " +
        "-1.7976931348623157E308, -3.4028235E38, [38, 37, 36, 35, 34, 33, 32, 31], \"drill\"}", map);
    assertFalse(rdr.next());
    results.clear();
  }
}
