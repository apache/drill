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

import static org.apache.drill.test.TestBuilder.listOf;
import static org.apache.drill.test.TestBuilder.mapOf;

import org.apache.drill.categories.MongoStorageTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.exec.ExecConstants;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Ignore("DRILL-3775")
@Category({SlowTest.class, MongoStorageTest.class})
public class TestMongoProjectPushDown extends MongoTestBase {

  /**
   *
   * @throws Exception
   */
  @Test
  public void testComplexProjectPushdown() throws Exception {

    try {
      testBuilder()
          .sqlQuery("select t.field_4.inner_3 as col_1, t.field_4 as col_2 from mongo.employee.schema_change t")
          .unOrdered()
          .optionSettingQueriesForTestQuery(String.format("alter session set `%s` = true", ExecConstants.MONGO_READER_READ_NUMBERS_AS_DOUBLE))
              .baselineColumns("col_1", "col_2")
              .baselineValues(
                  mapOf(),
                  mapOf(
                      "inner_1", listOf(),
                      "inner_3", mapOf()))
              .baselineValues(
                  mapOf("inner_object_field_1", 2.0),
                  mapOf(
                      "inner_1", listOf(1.0, 2.0, 3.0),
                      "inner_2", 3.0,
                      "inner_3", mapOf("inner_object_field_1", 2.0)))
              .baselineValues(
                  mapOf(),
                  mapOf(
                      "inner_1", listOf(4.0, 5.0, 6.0),
                      "inner_2", 3.0,
                      "inner_3", mapOf()))
              .go();
    } finally {
      test(String.format("alter session set `%s` = false", ExecConstants.MONGO_READER_READ_NUMBERS_AS_DOUBLE));
    }
  }

  @Test
  public void testSingleColumnProject() throws Exception {
    String query = String.format(TEST_QUERY_PROJECT_PUSH_DOWN_TEMPLATE_1,
        EMPLOYEE_DB, EMPINFO_COLLECTION);
    String expectedColNames = " \"columns\" : [ \"`employee_id`\" ]";
    testHelper(query, expectedColNames, 19);
  }

  @Test
  public void testMultipleColumnsProject() throws Exception {
    String query = String.format(TEST_QUERY_PROJECT_PUSH_DOWN__TEMPLATE_2,
        EMPLOYEE_DB, EMPINFO_COLLECTION);
    String expectedColNames = "\"columns\" : [ \"`employee_id`\", \"`rating`\" ]";
    testHelper(query, expectedColNames, 19);
  }

  @Test
  public void testStarProject() throws Exception {
    String query = String.format(TEST_QUERY_PROJECT_PUSH_DOWN__TEMPLATE_3,
        EMPLOYEE_DB, EMPINFO_COLLECTION);
    String expectedColNames = "*";
    testHelper(query, expectedColNames, 19);
  }

}
