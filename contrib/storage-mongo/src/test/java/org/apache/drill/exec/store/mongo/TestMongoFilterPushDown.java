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

public class TestMongoFilterPushDown extends MongoTestBase {

  @Test
  public void testFilterPushDownIsEqual() throws Exception {
    String queryString = String.format(
        TEST_FILTER_PUSH_DOWN_EQUAL_QUERY_TEMPLATE_1, EMPLOYEE_DB,
        EMPINFO_COLLECTION);
    String expectedExpr = "\"$eq\" : 52.17";
    testHelper(queryString, expectedExpr, 1);
  }

  @Test
  public void testFilterPushDownLessThanWithSingleField() throws Exception {
    String queryString = String.format(
        TEST_FILTER_PUSH_DOWN_LESS_THAN_QUERY_TEMPLATE_1, EMPLOYEE_DB,
        EMPINFO_COLLECTION);
    String expectedExpr = "\"$lt\" : 52.17";
    testHelper(queryString, expectedExpr, 9);
  }

  @Test
  public void testFilterPushDownGreaterThanWithSingleField() throws Exception {
    String queryString = String.format(
        TEST_FILTER_PUSH_DOWN_GREATER_THAN_QUERY_TEMPLATE_1, EMPLOYEE_DB,
        EMPINFO_COLLECTION);
    String expectedExpr = "\"$gt\" : 52.17";
    testHelper(queryString, expectedExpr, 9);
  }

}
