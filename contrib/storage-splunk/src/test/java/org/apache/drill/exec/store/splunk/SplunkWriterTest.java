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

package org.apache.drill.exec.store.splunk;

import org.apache.drill.categories.SlowTest;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.DirectRowSet;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.JVM)
@Category({SlowTest.class})
public class SplunkWriterTest extends SplunkBaseTest {

  @Test
  public void testBasicCTAS() throws Exception {

    // Verify that there is no index called t1 in Splunk
    String sql = "SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_SCHEMA = 'splunk' AND TABLE_NAME LIKE 't1'";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    assertEquals(0, results.rowCount());
    results.clear();

    // Now create the table
    sql = "CREATE TABLE `splunk`.`t1` AS SELECT * FROM cp.`test_data.csvh`";
    QuerySummary summary = client.queryBuilder().sql(sql).run();
    assertTrue(summary.succeeded());

    // Verify that an index was created called t1 in Splunk
    sql = "SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_SCHEMA = 'splunk' AND TABLE_NAME LIKE 't1'";
    results = client.queryBuilder().sql(sql).rowSet();
    assertEquals(1, results.rowCount());
    results.clear();

    // There seems to be some delay between the Drill query writing the data and the data being made
    // accessible.
    Thread.sleep(30000);

    // Next verify that the results arrived.
    sql = "SELECT clientip, categoryId FROM splunk.`t1`";
    results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("clientip", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .add("categoryId", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("198.35.2.120", "ACCESSORIES")
      .addRow("198.35.2.120", null)
      .addRow("198.35.2.120", null)
      .addRow("198.35.2.120", "STRATEGY")
      .addRow("198.35.2.120", "NULL")
      .build();
    RowSetUtilities.verify(expected, results);

    // Now drop the index
    sql = "DROP TABLE splunk.`t1`";
    summary = client.queryBuilder().sql(sql).run();
    assertTrue(summary.succeeded());

    // Verify that the index was deleted.
    sql = "SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_SCHEMA = 'splunk' AND TABLE_NAME LIKE 't1'";
    results = client.queryBuilder().sql(sql).rowSet();
    assertEquals(0, results.rowCount());
    results.clear();
  }

  @Test
  @Ignore("Run manually because of an unpredictable delay before newly inserted data gets indexed.")
  public void testBasicCTASWithScalarDataTypes() throws Exception {
    String query = "CREATE TABLE splunk.t2 AS " +
      "SELECT CAST(1 AS INTEGER) AS int_field," +
      "CAST(2 AS BIGINT) AS bigint_field," +
      "CAST(3.0 AS FLOAT) AS float4_field," +
      "CAST(4.0 AS DOUBLE) AS float8_field," +
      "'5.0' AS varchar_field," +
      "CAST('2021-01-01' AS DATE) as date_field," +
      "CAST('12:00:00' AS TIME) as time_field, " +
      "CAST('2015-12-30 22:55:55.23' AS TIMESTAMP) as timestamp_field, true AS boolean_field " +
      "FROM (VALUES(1))";
    // Create the table and insert the values
    QuerySummary insertResults = queryBuilder().sql(query).run();
    assertTrue(insertResults.succeeded());
    Thread.sleep(15000);

    // Query the table to see if the insertion was successful
    String testQuery = "SELECT int_field, bigint_field, float4_field, float8_field, varchar_field," +
      "date_field, time_field, timestamp_field, boolean_field FROM splunk.t2";
    DirectRowSet results = queryBuilder().sql(testQuery).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("int_field", MinorType.VARCHAR)
      .addNullable("bigint_field", MinorType.VARCHAR)
      .addNullable("float4_field", MinorType.VARCHAR)
      .addNullable("float8_field", MinorType.VARCHAR)
      .addNullable("varchar_field", MinorType.VARCHAR)
      .addNullable("date_field", MinorType.VARCHAR)
      .addNullable("time_field", MinorType.VARCHAR)
      .addNullable("timestamp_field", MinorType.VARCHAR)
      .addNullable("boolean_field", MinorType.VARCHAR)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("1", "2", "3.0", "4.0", "5.0", "2021-01-01", "12:00", "2015-12-30T22:55:55.230", "true")
      .build();

    RowSetUtilities.verify(expected, results);

    // Now drop the table
    String dropQuery = "DROP TABLE splunk.t2";
    QuerySummary dropResults = queryBuilder().sql(dropQuery).run();
    assertTrue(dropResults.succeeded());
  }

  @Test
  public void testInsert() throws Exception {

    // Create the table
    String sql = "CREATE TABLE `splunk`.`t3` AS SELECT * FROM cp.`test_data.csvh`";
    QuerySummary summary = client.queryBuilder().sql(sql).run();
    assertTrue(summary.succeeded());

    Thread.sleep(30000);

    // Now insert more records
    sql = "INSERT INTO `splunk`.`t3`  SELECT * FROM cp.`test_data2.csvh`";
    summary = client.queryBuilder().sql(sql).run();
    assertTrue(summary.succeeded());

    // There seems to be some delay between the Drill query writing the data and the data being made
    // accessible.
    Thread.sleep(30000);

    // Next verify that the results arrived.
    sql = "SELECT COUNT(*) as row_count FROM splunk.`t3`";
    long resultCount = client.queryBuilder().sql(sql).singletonLong();
    assertEquals(15L, resultCount);

    // Now drop the index
    sql = "DROP TABLE splunk.`t3`";
    summary = client.queryBuilder().sql(sql).run();
    assertTrue(summary.succeeded());
  }

  @Test
  public void testComplexFields() throws Exception {
    String sql = "CREATE TABLE `splunk`.`t4` AS SELECT record FROM cp.`schema_test.json`";
    QuerySummary summary = client.queryBuilder().sql(sql).run();
    assertTrue(summary.succeeded());

    Thread.sleep(30000);

    sql = "SELECT COUNT(*) FROM splunk.t4";
    long resultCount = client.queryBuilder().sql(sql).singletonLong();
    assertEquals(1L, resultCount);

    // Now drop the index
    sql = "DROP TABLE splunk.`t4`";
    summary = client.queryBuilder().sql(sql).run();
    assertTrue(summary.succeeded());
  }
}
