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
package org.apache.drill.exec.store.ltsv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLTSVQueries extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
  }

  @Test
  public void testWildcard() throws Exception {
    String sql = "SELECT * FROM cp.`simple.ltsv`";
    RowSet results  = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("referer", MinorType.VARCHAR)
        .addNullable("vhost", MinorType.VARCHAR)
        .addNullable("size", MinorType.VARCHAR)
        .addNullable("forwardedfor", MinorType.VARCHAR)
        .addNullable("reqtime", MinorType.VARCHAR)
        .addNullable("apptime", MinorType.VARCHAR)
        .addNullable("host", MinorType.VARCHAR)
        .addNullable("ua", MinorType.VARCHAR)
        .addNullable("req", MinorType.VARCHAR)
        .addNullable("status", MinorType.VARCHAR)
        .build();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("-", "api.example.com", "4968", "-", "2.532", "2.532", "xxx.xxx.xxx.xxx", "Java/1.8.0_131", "GET /v1/xxx HTTP/1.1", "200")
        .addRow("-", "api.example.com", "412", "-", "3.580", "3.580", "xxx.xxx.xxx.xxx", "Java/1.8.0_201", "GET /v1/yyy HTTP/1.1", "200")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testSelectColumns() throws Exception {
    String sql = "SELECT ua, reqtime FROM cp.`simple.ltsv`";
    RowSet results  = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("ua", MinorType.VARCHAR)
        .addNullable("reqtime", MinorType.VARCHAR)
        .build();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("Java/1.8.0_131", "2.532")
        .addRow("Java/1.8.0_201", "3.580")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testQueryWithConditions() throws Exception {
    String sql = "SELECT * FROM cp.`simple.ltsv` WHERE reqtime > 3.0";
    RowSet results  = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("referer", MinorType.VARCHAR)
        .addNullable("vhost", MinorType.VARCHAR)
        .addNullable("size", MinorType.VARCHAR)
        .addNullable("forwardedfor", MinorType.VARCHAR)
        .addNullable("reqtime", MinorType.VARCHAR)
        .addNullable("apptime", MinorType.VARCHAR)
        .addNullable("host", MinorType.VARCHAR)
        .addNullable("ua", MinorType.VARCHAR)
        .addNullable("req", MinorType.VARCHAR)
        .addNullable("status", MinorType.VARCHAR)
        .build();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("-", "api.example.com", "412", "-", "3.580", "3.580", "xxx.xxx.xxx.xxx", "Java/1.8.0_201", "GET /v1/yyy HTTP/1.1", "200")
        .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testSerDe() throws Exception {
    String sql = "SELECT COUNT(*) as cnt FROM cp.`simple.ltsv` ";
    String plan = queryBuilder().sql(sql).explainJson();
    long cnt = queryBuilder().physical(plan).singletonLong();
    assertEquals("Counts should match", 2L, cnt);
  }

  @Test
  public void testSkipEmptyLines() throws Exception {
    assertEquals(2, queryBuilder().sql("SELECT * FROM cp.`emptylines.ltsv`").run().recordCount());
  }

  @Test
  public void testReadException() throws Exception {
    try {
      run("SELECT * FROM table(cp.`invalid.ltsv` (type => 'ltsv', parseMode => 'strict'))");
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("DATA_READ ERROR: Empty key detected at line [0] position [49]"));
    }
  }

  @Test
  public void testProvidedSchema() throws Exception {
    String sql = "SELECT * FROM table(cp.`simple.ltsv` (type=> 'ltsv', schema => 'inline=(`referer` VARCHAR, `vhost` VARCHAR, `size` INT, `forwardedfor` VARCHAR, " +
        "`reqtime` DOUBLE, `apptime` DOUBLE, `status` INT)'))";
    RowSet results  = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("referer", MinorType.VARCHAR)
        .addNullable("vhost", MinorType.VARCHAR)
        .addNullable("size", MinorType.INT)
        .addNullable("forwardedfor", MinorType.VARCHAR)
        .addNullable("reqtime", MinorType.FLOAT8)
        .addNullable("apptime", MinorType.FLOAT8)
        .addNullable("status", MinorType.INT)
        .addNullable("host", MinorType.VARCHAR)
        .addNullable("ua", MinorType.VARCHAR)
        .addNullable("req", MinorType.VARCHAR)
        .build();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("-", "api.example.com", 4968, "-", 2.532, 2.532, 200, "xxx.xxx.xxx.xxx", "Java/1.8.0_131", "GET /v1/xxx HTTP/1.1")
        .addRow("-", "api.example.com", 412, "-", 3.58, 3.58, 200, "xxx.xxx.xxx.xxx", "Java/1.8.0_201", "GET /v1/yyy HTTP/1.1")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }
}
