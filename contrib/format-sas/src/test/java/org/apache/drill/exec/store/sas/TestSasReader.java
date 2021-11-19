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

package org.apache.drill.exec.store.sas;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;


public class TestSasReader extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));

    // Needed for compressed file unit test
    dirTestWatcher.copyResourceToRoot(Paths.get("sas/"));
  }

  @Test
  public void testStarQuery() throws Exception {
    String sql = "SELECT * FROM cp.`sas/mixed_data_two.sas7bdat` WHERE x1 = 1";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("x1", MinorType.BIGINT)
      .addNullable("x2", MinorType.FLOAT8)
      .addNullable("x3", MinorType.VARCHAR)
      .addNullable("x4", MinorType.FLOAT8)
      .addNullable("x5", MinorType.FLOAT8)
      .addNullable("x6", MinorType.FLOAT8)
      .addNullable("x7", MinorType.FLOAT8)
      .addNullable("x8", MinorType.FLOAT8)
      .addNullable("x9", MinorType.FLOAT8)
      .addNullable("x10", MinorType.FLOAT8)
      .addNullable("x11", MinorType.FLOAT8)
      .addNullable("x12", MinorType.FLOAT8)
      .addNullable("x13", MinorType.FLOAT8)
      .addNullable("x14", MinorType.FLOAT8)
      .addNullable("x15", MinorType.BIGINT)
      .addNullable("x16", MinorType.BIGINT)
      .addNullable("x17", MinorType.BIGINT)
      .addNullable("x18", MinorType.BIGINT)
      .addNullable("x19", MinorType.BIGINT)
      .addNullable("x20", MinorType.BIGINT)
      .addNullable("x21", MinorType.BIGINT)
      .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(1L, 1.1, "AAAAAAAA", 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 31626061L, 31625961L, 31627061L, 31616061L, 31636061L, 31526061L, 31726061L)
      .addRow(1L, 1.1, "AAAAAAAA", 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 31626061L, 31625961L, 31627061L, 31616061L, 31636061L, 31526061L, 31726061L)
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testDates() throws Exception {
    String sql = "SELECT * FROM cp.`sas/date_formats.sas7bdat`";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();
    results.print();

  }

    @Test
  public void testSerDe() throws Exception {
    String sql = "SELECT COUNT(*) as cnt FROM cp.`sas/mixed_data_two.sas7bdat` ";
    String plan = queryBuilder().sql(sql).explainJson();
    long cnt = queryBuilder().physical(plan).singletonLong();
    assertEquals("Counts should match",50L, cnt);
  }

  @Test
  public void testLimitPushdown() throws Exception {
    String sql = "SELECT * FROM cp.`sas/mixed_data_one.sas7bdat` LIMIT 5";

    queryBuilder()
      .sql(sql)
      .planMatcher()
      .include("Limit", "maxRecords=5")
      .match();
  }
}
