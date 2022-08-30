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

package org.apache.drill.exec.store.fixedwidth;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

import static org.apache.drill.test.QueryTestUtil.generateCompressedFile;
import static org.junit.Assert.assertEquals;

@Category(RowSetTests.class)
public class TestFixedWidthRecordReader extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));

    FixedWidthFormatConfig formatConfig = new FixedWidthFormatConfig(Lists.newArrayList("fwf"),
      Lists.newArrayList(
        new FixedWidthFieldConfig("Number", 1, 5, TypeProtos.MinorType.VARDECIMAL),
        new FixedWidthFieldConfig("Address", 12, 3, TypeProtos.MinorType.INT),
        new FixedWidthFieldConfig("Letter", 7, 4, TypeProtos.MinorType.VARCHAR),
        new FixedWidthFieldConfig("Date", 16, 10, TypeProtos.MinorType.DATE,  "MM-dd-yyyy"),
        new FixedWidthFieldConfig("Time", 27, 8, TypeProtos.MinorType.TIME,"HH:mm:ss"),
        new FixedWidthFieldConfig("DateTime", 36, 23, TypeProtos.MinorType.TIMESTAMP, "MM-dd-yyyy'T'HH:mm:ss.SSX")
      ));
    cluster.defineFormat("dfs", "fwf", formatConfig);
    cluster.defineFormat("cp", "fwf", formatConfig);

    // Needed for compressed file unit test
    dirTestWatcher.copyResourceToRoot(Paths.get("fwf/"));
  }

  @Test
  public void testStarQuery() throws Exception {
    String sql = "SELECT * FROM cp.`fwf/test.fwf`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    RowSet expected = setupTestData();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testExplicitAllQuery() throws Exception {
    String sql = "SELECT Number, Letter, Address, `Date`, `Time`, DateTime FROM cp.`fwf/test.fwf`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    RowSet expected = setupTestData();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testExplicitQuery() throws Exception {
    String sql = "SELECT Number, Letter, Address FROM cp.`fwf/test.fwf` WHERE Letter='yzzz'";
    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("Number", TypeProtos.MinorType.VARDECIMAL,38,4)
      .addNullable("Letter", TypeProtos.MinorType.VARCHAR)
      .addNullable("Address", TypeProtos.MinorType.INT)
      .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(77.77, "yzzz", 777)
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  //Test Serialization/Deserialization
  @Test
  public void testSerDe() throws Exception {
    String sql = "SELECT COUNT(*) FROM cp.`fwf/test.fwf`";
    String plan = queryBuilder().sql(sql).explainJson();
    long cnt = queryBuilder().physical(plan).singletonLong();
    assertEquals(25L, cnt);
  }

  @Test
  public void testStarQueryWithCompressedFile() throws Exception {
    generateCompressedFile("fwf/test.fwf", "zip", "fwf/test.fwf.zip" );

    String sql = "SELECT * FROM dfs.`fwf/test.fwf.zip`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    RowSet expected = setupTestData();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  // Test Entering invalid schemata - incorrect limits
    // Undefined field, what happens
    // Parse invalid file, make sure correct error


  @Test
  public void testOutOfOrder() throws Exception{
    String sql = "SELECT Address, DateTime, `Date`, Letter FROM cp.`fwf/test.fwf`";
    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("Address", TypeProtos.MinorType.INT)
      .addNullable("DateTime", TypeProtos.MinorType.TIMESTAMP)
      .addNullable("Date", TypeProtos.MinorType.DATE)
      .addNullable("Letter", TypeProtos.MinorType.VARCHAR)
      .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(567, Instant.parse("2021-02-10T15:30:27.00Z"), LocalDate.parse("2021-02-10"), "test")
      .addRow(890, Instant.parse("2021-07-27T16:40:15.00Z"), LocalDate.parse("2021-07-27"), "TEST")
      .addRow(111, Instant.parse("1111-11-11T16:28:43.11Z"), LocalDate.parse("1111-11-11"), "abcd")
      .addRow(222, Instant.parse("2222-01-23T03:22:22.22Z"), LocalDate.parse("2222-01-22"), "efgh")
      .addRow(333, Instant.parse("3333-02-01T06:33:33.33Z"), LocalDate.parse("3333-02-01"), "ijkl")
      .addRow(444, Instant.parse("4444-03-02T07:44:44.44Z"), LocalDate.parse("4444-03-02"), "mnop")
      .addRow(555, Instant.parse("5555-04-03T07:55:55.55Z"), LocalDate.parse("5555-04-03"), "qrst")
      .addRow(666, Instant.parse("6666-05-04T08:01:01.01Z"), LocalDate.parse("6666-05-04"), "uvwx")
      .addRow(777, Instant.parse("7777-06-05T09:11:11.11Z"), LocalDate.parse("7777-06-05"), "yzzz")
      .addRow(888, Instant.parse("8888-07-07T10:22:22.22Z"), LocalDate.parse("8888-07-06"), "aabb")
      .addRow(888, Instant.parse("8888-07-07T10:22:22.22Z"), LocalDate.parse("8888-07-06"), "aabb")
      .addRow(888, Instant.parse("8888-07-07T10:22:22.22Z"), LocalDate.parse("8888-07-06"), "aabb")
      .addRow(888, Instant.parse("8888-07-07T10:22:22.22Z"), LocalDate.parse("8888-07-06"), "aabb")
      .addRow(888, Instant.parse("8888-07-07T10:22:22.22Z"), LocalDate.parse("8888-07-06"), "aabb")
      .addRow(888, Instant.parse("8888-07-07T10:22:22.22Z"), LocalDate.parse("8888-07-06"), "aabb")
      .addRow(888, Instant.parse("8888-07-07T10:22:22.22Z"), LocalDate.parse("8888-07-06"), "aabb")
      .addRow(888, Instant.parse("8888-07-07T10:22:22.22Z"), LocalDate.parse("8888-07-06"), "aabb")
      .addRow(888, Instant.parse("8888-07-07T10:22:22.22Z"), LocalDate.parse("8888-07-06"), "aabb")
      .addRow(888, Instant.parse("8888-07-07T10:22:22.22Z"), LocalDate.parse("8888-07-06"), "aabb")
      .addRow(888, Instant.parse("8888-07-07T10:22:22.22Z"), LocalDate.parse("8888-07-06"), "aabb")
      .addRow(888, Instant.parse("8888-07-07T10:22:22.22Z"), LocalDate.parse("8888-07-06"), "aabb")
      .addRow(888, Instant.parse("8888-07-07T10:22:22.22Z"), LocalDate.parse("8888-07-06"), "aabb")
      .addRow(888, Instant.parse("8888-07-07T10:22:22.22Z"), LocalDate.parse("8888-07-06"), "aabb")
      .addRow(888, Instant.parse("8888-07-07T10:22:22.22Z"), LocalDate.parse("8888-07-06"), "aabb")
      .addRow(888, Instant.parse("8888-07-07T10:22:22.22Z"), LocalDate.parse("8888-07-06"), "aabb")
      .build();

      new RowSetComparison(expected).verifyAndClearAll(results);
  }

  // How should we be handling an empty/blank row?
  @Test
  public void testEmptyRow() throws Exception {
    String sql = "SELECT * FROM cp.`fwf/test_blankrow.fwf`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    RowSet expected = setupTestData();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  // Create unit test for overloaded constructor

  private RowSet setupTestData(){
    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("Number", TypeProtos.MinorType.VARDECIMAL,38,4)
      .addNullable("Letter", TypeProtos.MinorType.VARCHAR)
      .addNullable("Address", TypeProtos.MinorType.INT)
      .addNullable("Date", TypeProtos.MinorType.DATE)
      .addNullable("Time", TypeProtos.MinorType.TIME)
      .addNullable("DateTime", TypeProtos.MinorType.TIMESTAMP)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(12.34, "test", 567, LocalDate.parse("2021-02-10"), LocalTime.parse("10:30:27"), Instant.parse("2021-02-10T15:30:27.00Z"))
      .addRow(56.78, "TEST", 890, LocalDate.parse("2021-07-27"), LocalTime.parse("12:40:15"), Instant.parse("2021-07-27T16:40:15.00Z"))
      .addRow(11.11, "abcd", 111, LocalDate.parse("1111-11-11"), LocalTime.parse("11:11:11"), Instant.parse("1111-11-11T16:28:43.11Z"))
      .addRow(22.22, "efgh", 222, LocalDate.parse("2222-01-22"), LocalTime.parse("22:22:22"), Instant.parse("2222-01-23T03:22:22.22Z"))
      .addRow(33.33, "ijkl", 333, LocalDate.parse("3333-02-01"), LocalTime.parse("01:33:33"), Instant.parse("3333-02-01T06:33:33.33Z"))
      .addRow(44.44, "mnop", 444, LocalDate.parse("4444-03-02"), LocalTime.parse("02:44:44"), Instant.parse("4444-03-02T07:44:44.44Z"))
      .addRow(55.55, "qrst", 555, LocalDate.parse("5555-04-03"), LocalTime.parse("03:55:55"), Instant.parse("5555-04-03T07:55:55.55Z"))
      .addRow(66.66, "uvwx", 666, LocalDate.parse("6666-05-04"), LocalTime.parse("04:01:01"), Instant.parse("6666-05-04T08:01:01.01Z"))
      .addRow(77.77, "yzzz", 777, LocalDate.parse("7777-06-05"), LocalTime.parse("05:11:11"), Instant.parse("7777-06-05T09:11:11.11Z"))
      .addRow(88.88, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(88.88, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(88.88, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(88.88, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(88.88, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(88.88, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(88.88, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(88.88, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(88.88, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(88.88, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(88.88, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(88.88, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(88.88, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(88.88, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(88.88, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(88.88, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .build();

    return expected;
  }

}
