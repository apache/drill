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

package org.apache.drill.exec.store.druid;

import org.apache.drill.categories.DruidStorageTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;


@Ignore("These tests require a running druid instance. You may start druid by using the docker-compose provide in resources/druid and enable these tests")
@Category({SlowTest.class, DruidStorageTest.class})
public class TestDruidQueries extends DruidTestBase {

  @Test
  public void testStarQuery() throws Exception {
    testBuilder()
      .sqlQuery(String.format(TEST_STAR_QUERY, TEST_DATASOURCE_WIKIPEDIA))
      .unOrdered()
      .expectsNumRecords(876)
      .go();
  }

  @Test
  public void testEqualsFilter() throws Exception {
    testBuilder()
      .sqlQuery(String.format(TEST_STRING_EQUALS_FILTER_QUERY_TEMPLATE1, TEST_DATASOURCE_WIKIPEDIA))
      .unOrdered()
      .expectsNumRecords(2)
      .go();
  }

  @Test
  public void testTwoANDdEqualsFilter() throws Exception {
    testBuilder()
        .sqlQuery(String.format(TEST_STRING_TWO_AND_EQUALS_FILTER_QUERY_TEMPLATE1, TEST_DATASOURCE_WIKIPEDIA))
        .unOrdered()
        .expectsNumRecords(1)
        .go();
  }

  @Test
  public void testTwoOrdEqualsFilter() throws Exception {
    testBuilder()
        .sqlQuery(String.format(TEST_STRING_TWO_OR_EQUALS_FILTER_QUERY_TEMPLATE1, TEST_DATASOURCE_WIKIPEDIA))
        .unOrdered()
        .expectsNumRecords(1)
        .go();
  }

  @Test
  public void testSingleColumnProject() throws Exception {
    String query = String.format(TEST_QUERY_PROJECT_PUSH_DOWN_TEMPLATE_1, TEST_DATASOURCE_WIKIPEDIA);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("comment")
        .expectsNumRecords(876)
        .go();
  }

  @Test
  public void testCountAllRowsQuery() throws Exception {
    String query = String.format(TEST_QUERY_COUNT_QUERY_TEMPLATE, TEST_DATASOURCE_WIKIPEDIA);

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("mycount")
      .baselineValues(876L)
      .go();
  }

  @Test
  public void testGroupByQuery() throws Exception {
    String sql = String.format("SELECT `namespace`, COUNT(*) AS user_count FROM druid.`%s` GROUP BY `namespace` ORDER BY user_count DESC LIMIT 5",TEST_DATASOURCE_WIKIPEDIA);
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("namespace", MinorType.VARCHAR, DataMode.OPTIONAL)
        .add("user_count", MinorType.BIGINT)
        .buildSchema();

    RowSet expected = client.rowSetBuilder(expectedSchema)
        .addRow("Main", 702)
        .addRow("User talk", 29)
        .addRow("Wikipedia", 26)
        .addRow("Talk", 17)
        .addRow("User", 12)
        .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testSerDe() throws Exception {
    // TODO Start here... filters are not deserializing properly

    String sql = String.format("SELECT COUNT(*) FROM druid.`%s`", TEST_DATASOURCE_WIKIPEDIA);
    String plan = queryBuilder().sql(sql).explainJson();
    long cnt = queryBuilder().physical(plan).singletonLong();
    assertEquals("Counts should match", 876L, cnt);
  }
}
