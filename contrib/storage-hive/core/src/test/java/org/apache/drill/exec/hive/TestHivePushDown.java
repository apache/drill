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
package org.apache.drill.exec.hive;

import org.apache.drill.categories.HiveStorageTest;
import org.apache.drill.categories.SlowTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;


@Category({SlowTest.class, HiveStorageTest.class})
public class TestHivePushDown extends HiveTestBase {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testLimitPushDown() throws Exception {
    String query = "SELECT * FROM hive.`default`.kv LIMIT 1";

    int actualRowCount = testSql(query);
    assertEquals("Expected and actual row count should match", 1, actualRowCount);

    testPlanMatchingPatterns(query, new String[]{"LIMIT"}, new String[]{"maxRecords=1"});
  }
}
