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
package org.apache.drill.exec.store.mongo;

import org.apache.drill.categories.MongoStorageTest;
import org.apache.drill.categories.SlowTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SlowTest.class, MongoStorageTest.class})
public class TestMongoQueries extends MongoTestBase {

  @Test
  public void testBooleanFilter() throws Exception {
    testBuilder()
        .sqlQuery(String.format(TEST_BOOLEAN_FILTER_QUERY_TEMPLATE1, EMPLOYEE_DB, EMPINFO_COLLECTION))
        .unOrdered()
        .expectsNumRecords(11)
        .go();

    testBuilder()
        .sqlQuery(String.format(TEST_BOOLEAN_FILTER_QUERY_TEMPLATE2, EMPLOYEE_DB, EMPINFO_COLLECTION))
        .unOrdered()
        .expectsNumRecords(8)
        .go();
  }

  @Test
  public void testWithANDOperator() throws Exception {
    testBuilder()
        .sqlQuery(String.format(TEST_BOOLEAN_FILTER_QUERY_TEMPLATE3, EMPLOYEE_DB, EMPINFO_COLLECTION))
        .unOrdered()
        .expectsNumRecords(4)
        .go();
  }

  @Test
  public void testWithOROperator() throws Exception {
    testBuilder()
        .sqlQuery(String.format(TEST_BOOLEAN_FILTER_QUERY_TEMPLATE3, EMPLOYEE_DB, EMPINFO_COLLECTION))
        .unOrdered()
        .expectsNumRecords(4)
        .go();
  }

  @Test
  public void testResultCount() throws Exception {
    testBuilder()
        .sqlQuery(String.format(TEST_BOOLEAN_FILTER_QUERY_TEMPLATE4, EMPLOYEE_DB, EMPINFO_COLLECTION))
        .unOrdered()
        .expectsNumRecords(5)
        .go();
  }

  @Test
  public void testUnShardedDBInShardedCluster() throws Exception {
    testBuilder()
        .sqlQuery(String.format(TEST_STAR_QUERY_UNSHARDED_DB, DONUTS_DB, DONUTS_COLLECTION))
        .unOrdered()
        .expectsNumRecords(5)
        .go();
  }

  @Test
  public void testEmptyCollection() throws Exception {
    testBuilder()
        .sqlQuery(String.format(TEST_STAR_QUERY_UNSHARDED_DB, EMPLOYEE_DB, EMPTY_COLLECTION))
        .unOrdered()
        .expectsNumRecords(0)
        .go();
  }

  @Test
  @Ignore("DRILL-7428") // Query is invalid, Drill bug allows it.
  public void testUnShardedDBInShardedClusterWithProjectionAndFilter() throws Exception {
    testBuilder()
        .sqlQuery(String.format(TEST_STAR_QUERY_UNSHARDED_DB_PROJECT_FILTER, DONUTS_DB, DONUTS_COLLECTION))
        .unOrdered()
        .expectsNumRecords(2)
        .go();
  }

  @Test
  @Ignore("DRILL-7428") // Query is invalid, Drill bug allows it.
  public void testUnShardedDBInShardedClusterWithGroupByProjectionAndFilter() throws Exception {
    testBuilder()
        .sqlQuery(String.format(TEST_STAR_QUERY_UNSHARDED_DB_GROUP_PROJECT_FILTER, DONUTS_DB, DONUTS_COLLECTION))
        .unOrdered()
        .expectsNumRecords(5)
        .go();
  }
}
