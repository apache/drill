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

package org.apache.drill.exec.store.json;

import org.apache.drill.categories.RowSetTests;

import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.DirectRowSet;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(RowSetTests.class)
public class TestJsonModes extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
  }

  @Test
  public void testAllTextMode() throws Exception {
    String sql = "SELECT `integer`, `float` FROM cp.`jsoninput/input2.json`";
    RowSet results  = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("integer", MinorType.BIGINT)
      .addNullable("float", MinorType.FLOAT8)
      .build();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(2010, 17.4)
      .addRow(-2002, -1.2)
      .addRow(2001, 1.2)
      .addRow(6005, 1.2)
      .build();
    new RowSetComparison(expected).verifyAndClearAll(results);

    // Now try with all text mode
    sql = "SELECT `integer`, `float` FROM table(cp.`jsoninput/input2.json` (type => 'json', allTextMode => True))";
    results  = client.queryBuilder().sql(sql).rowSet();
    expectedSchema = new SchemaBuilder()
      .addNullable("integer", MinorType.VARCHAR)
      .addNullable("float", MinorType.VARCHAR)
      .build();

    expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("2010", "17.4")
      .addRow("-2002", "-1.2")
      .addRow("2001", "1.2")
      .addRow("6005", "1.2")
      .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testReadDoubles() throws Exception {
    String sql = "SELECT `integer`, `float` FROM table(cp.`jsoninput/input2.json` (type => 'json', readNumbersAsDouble => True))";
    DirectRowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("integer", MinorType.FLOAT8)
      .addNullable("float", MinorType.FLOAT8)
      .build();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(2010.0, 17.4)
      .addRow(-2002.0, -1.2)
      .addRow(2001.0, 1.2)
      .addRow(6005.0, 1.2)
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testSkipInvalidRecords() throws Exception {
    String sql = "SELECT SUM(balance) FROM cp.`jsoninput/drill4653/file.json`";
    DirectRowSet results;
    try {
      client.queryBuilder().sql(sql).rowSet();
      fail();
    } catch (UserRemoteException e) {
      assertTrue(e.getMessage().contains("Error parsing JSON - Illegal unquoted character"));
    }

    sql = "SELECT SUM(balance) AS total FROM table(cp.`jsoninput/drill4653/file.json` (type => 'json', skipMalformedJSONRecords => True))";
    results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("total", MinorType.FLOAT8)
      .build();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(6003.9)
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testNanInf() throws Exception {
    String sql = "SELECT * FROM cp.`jsoninput/nan_test.json`";
    DirectRowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("nan_col", MinorType.FLOAT8)
      .addNullable("inf_col", MinorType.FLOAT8)
      .build();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(Double.NaN, Double.POSITIVE_INFINITY)
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);

    // Now disallow NaN
    sql = "SELECT * FROM table(cp.`jsoninput/nan_test.json` (type => 'json', nanInf => False))";
    try {
      client.queryBuilder().sql(sql).rowSet();
      fail();
    } catch (UserRemoteException e) {
      assertTrue(e.getMessage().contains("Error parsing JSON - Non-standard token 'NaN'"));
    }
  }

  @Test
  public void testSerDe() throws Exception {
    String sql = "SELECT COUNT(*) as cnt FROM cp.`jsoninput/input2.json`";
    String plan = queryBuilder().sql(sql).explainJson();
    long cnt = queryBuilder().physical(plan).singletonLong();
    assertEquals("Counts should match", 4L, cnt);
  }
}
