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

import static org.junit.Assert.assertEquals;

@Category(RowSetTests.class)
public class TestFixedwidthRecordReader extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
    FixedwidthFormatConfig formatConfig = new FixedwidthFormatConfig(Lists.newArrayList("fwf"),
      Lists.newArrayList(
        new FixedwidthFieldConfig(TypeProtos.MinorType.INT, "Number", "", 1, 4),
        new FixedwidthFieldConfig(TypeProtos.MinorType.VARCHAR, "Letter", "", 6, 4),
        new FixedwidthFieldConfig(TypeProtos.MinorType.INT, "Address", "", 11, 3),
        new FixedwidthFieldConfig(TypeProtos.MinorType.DATE, "Date", "MM-dd-yyyy", 15, 10),
        new FixedwidthFieldConfig(TypeProtos.MinorType.TIME, "Time", "HH:mm:ss", 26, 8),
        new FixedwidthFieldConfig(TypeProtos.MinorType.TIMESTAMP, "DateTime", "MM-dd-yyyy'T'HH:mm:ss.SSX", 35, 23)
      ));
    cluster.defineFormat("cp", "fwf", formatConfig);

    // Needed for compressed file unit test
    dirTestWatcher.copyResourceToRoot(Paths.get("fwf/"));
  }

  @Test
  public void testExplicitQuery() throws Exception {
    String sql = "SELECT ID, Urban, Urban_value FROM dfs.`spss/testdata.sav` WHERE d16=4";
    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("ID", TypeProtos.MinorType.FLOAT8)
      .addNullable("Urban", TypeProtos.MinorType.FLOAT8)
      .addNullable("Urban_value", TypeProtos.MinorType.VARCHAR)
      .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(47.0, 1.0, "Urban").addRow(53.0, 1.0, "Urban")
      .addRow(66.0, 1.0, "Urban")
      .build();
    assertEquals(3, results.rowCount());

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testBatchReader() throws Exception {
    String sql = "SELECT * FROM cp.`fwf/test.fwf` LIMIT 30";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("Number", TypeProtos.MinorType.INT)
      .addNullable("Letter", TypeProtos.MinorType.VARCHAR)
      .addNullable("Address", TypeProtos.MinorType.INT)
      .addNullable("Date", TypeProtos.MinorType.DATE)
      .addNullable("Time", TypeProtos.MinorType.TIME)
      .addNullable("DateTime", TypeProtos.MinorType.TIMESTAMP)
      .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(1234, "test", 567, LocalDate.parse("2021-02-10"), LocalTime.parse("10:30:27"), Instant.parse("2021-02-10T15:30:27.00Z"))
      .addRow(5678, "TEST", 890, LocalDate.parse("2021-07-27"), LocalTime.parse("12:40:15"), Instant.parse("2021-07-27T16:40:15.00Z"))
      .addRow(1111, "abcd", 111, LocalDate.parse("1111-11-11"), LocalTime.parse("11:11:11"), Instant.parse("1111-11-11T16:28:43.11Z"))
      .addRow(2222, "efgh", 222, LocalDate.parse("2222-01-22"), LocalTime.parse("22:22:22"), Instant.parse("2222-01-23T03:22:22.22Z"))
      .addRow(3333, "ijkl", 333, LocalDate.parse("3333-02-01"), LocalTime.parse("01:33:33"), Instant.parse("3333-02-01T06:33:33.33Z"))
      .addRow(4444, "mnop", 444, LocalDate.parse("4444-03-02"), LocalTime.parse("02:44:44"), Instant.parse("4444-03-02T07:44:44.44Z"))
      .addRow(5555, "qrst", 555, LocalDate.parse("5555-04-03"), LocalTime.parse("03:55:55"), Instant.parse("5555-04-03T07:55:55.55Z"))
      .addRow(6666, "uvwx", 666, LocalDate.parse("6666-05-04"), LocalTime.parse("04:01:01"), Instant.parse("6666-05-04T08:01:01.01Z"))
      .addRow(7777, "yzzz", 777, LocalDate.parse("7777-06-05"), LocalTime.parse("05:11:11"), Instant.parse("7777-06-05T09:11:11.11Z"))
      .addRow(8888, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(8888, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(8888, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(8888, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(8888, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(8888, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(8888, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(8888, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(8888, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(8888, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(8888, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(8888, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(8888, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(8888, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(8888, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .addRow(8888, "aabb", 888, LocalDate.parse("8888-07-06"), LocalTime.parse("06:22:22"), Instant.parse("8888-07-07T10:22:22.22Z"))
      .build();

    System.out.println(expected);
    assertEquals(25, results.rowCount());

    //System.out.println(results.batchSchema());
    System.out.println(results);

    new RowSetComparison(expected).verifyAndClearAll(results);
    System.out.println("Test complete.");
    client.close();
  }

}
