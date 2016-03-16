/**
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

import org.junit.Test;

public class TestMongoQueries extends MongoTestBase {

  @Test
  public void testBooleanFilter() throws Exception {
    String queryString = String.format(TEST_BOOLEAN_FILTER_QUERY_TEMPLATE1,
        EMPLOYEE_DB, EMPINFO_COLLECTION);
    runMongoSQLVerifyCount(queryString, 11);
    queryString = String.format(TEST_BOOLEAN_FILTER_QUERY_TEMPLATE2,
        EMPLOYEE_DB, EMPINFO_COLLECTION);
    runMongoSQLVerifyCount(queryString, 8);
  }

  @Test
  public void testWithANDOperator() throws Exception {
    String queryString = String.format(TEST_BOOLEAN_FILTER_QUERY_TEMPLATE3,
        EMPLOYEE_DB, EMPINFO_COLLECTION);
    runMongoSQLVerifyCount(queryString, 4);
  }

  @Test
  public void testWithOROperator() throws Exception {
    String queryString = String.format(TEST_BOOLEAN_FILTER_QUERY_TEMPLATE3,
        EMPLOYEE_DB, EMPINFO_COLLECTION);
    runMongoSQLVerifyCount(queryString, 4);
  }

  @Test
  public void testResultCount() throws Exception {
    String queryString = String.format(TEST_BOOLEAN_FILTER_QUERY_TEMPLATE4,
        EMPLOYEE_DB, EMPINFO_COLLECTION);
    runMongoSQLVerifyCount(queryString, 5);
  }

  @Test
  public void testUnShardedDBInShardedCluster() throws Exception {
    String queryString = String.format(TEST_STAR_QUERY_UNSHARDED_DB,
        DONUTS_DB, DONUTS_COLLECTION);
    runMongoSQLVerifyCount(queryString, 5);
  }

  @Test
  public void testEmptyCollection() throws Exception {
    String queryString = String.format(TEST_STAR_QUERY_UNSHARDED_DB, EMPLOYEE_DB, EMPTY_COLLECTION);
    runMongoSQLVerifyCount(queryString, 0);
  }

  @Test
  public void testUnShardedDBInShardedClusterWithProjectionAndFilter() throws Exception {
    String queryString = String.format(TEST_STAR_QUERY_UNSHARDED_DB_PROJECT_FILTER,
        DONUTS_DB, DONUTS_COLLECTION);
    runMongoSQLVerifyCount(queryString, 2);
  }

  @Test
  public void testUnShardedDBInShardedClusterWithGroupByProjectionAndFilter() throws Exception {
    String queryString = String.format(TEST_STAR_QUERY_UNSHARDED_DB_GROUP_PROJECT_FILTER,
        DONUTS_DB, DONUTS_COLLECTION);
    runMongoSQLVerifyCount(queryString, 5);
  }
}
