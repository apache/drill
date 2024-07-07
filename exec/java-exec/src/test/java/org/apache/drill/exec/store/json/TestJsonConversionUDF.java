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


import ch.qos.logback.classic.Level;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.project.ProjectRecordBatch;
import org.apache.drill.exec.physical.impl.validate.IteratorValidatorBatchIterator;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.LogFixture;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestJsonConversionUDF extends ClusterTest {

  protected static LogFixture logFixture;
  private final static Level CURRENT_LOG_LEVEL = Level.DEBUG;
  @BeforeClass
  public static void setup() throws Exception {
    logFixture = LogFixture.builder()
      .toConsole()
      .logger(ProjectRecordBatch.class, CURRENT_LOG_LEVEL)
      .logger(JsonLoaderImpl.class, CURRENT_LOG_LEVEL)
      .logger(IteratorValidatorBatchIterator.class, CURRENT_LOG_LEVEL)
      .build();

    startCluster(ClusterFixture.builder(dirTestWatcher));
  }

  @Test
  public void testConvertFromJsonVarBinary() throws Exception {
    client.alterSession(ExecConstants.JSON_READER_NAN_INF_NUMBERS, true);
    String sql = "SELECT string_binary(convert_toJSON(convert_fromJSON(columns[1]))) as col FROM cp.`jsoninput/nan_test.csv`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    assertEquals("Query result must contain 1 row", 1, results.rowCount());
    results.clear();
  }

  @Test
  public void testConvertFromJsonVarChar() throws Exception {
    String sql = "SELECT json_data['foo'] AS foo, json_data['num'] AS num FROM " +
        "(SELECT convert_FromJSON('{\"foo\":\"bar\", \"num\":10}') as json_data FROM (VALUES(1)))";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("foo", MinorType.VARCHAR)
        .addNullable("num", MinorType.BIGINT)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("bar", 10L)
        .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testMultipleRows() throws Exception {
    String sql = "SELECT string_binary(convert_toJSON(`name`)) FROM cp.`jsoninput/multirow.csvh`";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    results.print();
  }
}
