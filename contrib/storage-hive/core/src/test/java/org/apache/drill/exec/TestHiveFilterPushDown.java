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
package org.apache.drill.exec;

import org.apache.drill.categories.HiveStorageTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.exec.hive.HiveTestBase;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category({SlowTest.class, HiveStorageTest.class})
public class TestHiveFilterPushDown extends HiveTestBase {

    @BeforeClass
    public static void init() {
        //set false for test parquet push down
        setSessionOption(ExecConstants.HIVE_OPTIMIZE_PARQUET_SCAN_WITH_NATIVE_READER, false);
        setSessionOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY, true);
    }

    @AfterClass
    public static void cleanup() {
        resetSessionOption(ExecConstants.HIVE_OPTIMIZE_PARQUET_SCAN_WITH_NATIVE_READER);
        resetSessionOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY);
    }

    @Test
    public void testPushDownWithEqualForOrc() throws Exception {
        String query = "select * from hive.orc_push_down where key = 1";

        int actualRowCount = testSql(query);
        assertEquals("Expected and actual row count should match", 4, actualRowCount);

        testPlanMatchingPatterns(query,
                new String[]{"Filter\\(condition=\\[=\\($0, 1\\)\\]\\)",
                        "SearchArgument=leaf-0 = \\(EQUALS key 1\\), expr = leaf-0"},
                new String[]{});
    }

    @Test
    public void testPushDownWithNotEqualForOrc() throws Exception {
        String query = "select * from hive.orc_push_down where key <> 1";

        int actualRowCount = testSql(query);
        assertEquals("Expected and actual row count should match", 2, actualRowCount);

        testPlanMatchingPatterns(query,
                new String[]{"Filter\\(condition=\\[=\\<\\>\\($0, 1\\)\\]\\)",
                        "SearchArgument=leaf-0 = \\(EQUALS key 1\\), expr = \\(not leaf-0\\)"},
                new String[]{});
    }

    @Test
    public void testPushDownWithGreaterThanForOrc() throws Exception {
        String query = "select * from hive.orc_push_down where key > 1";

        int actualRowCount = testSql(query);
        assertEquals("Expected and actual row count should match", 2, actualRowCount);

        testPlanMatchingPatterns(query,
                new String[]{"Filter\\(condition=\\[=\\>\\($0, 1\\)\\]\\)",
                        "SearchArgument=leaf-0 = \\(LESS_THAN_EQUALS key 1\\), expr = \\(not leaf-0\\)"},
                new String[]{});
    }

    @Test
    public void testPushDownWithLessThanForOrc() throws Exception {
        String query = "select * from hive.orc_push_down where key < 2";

        int actualRowCount = testSql(query);
        assertEquals("Expected and actual row count should match", 4, actualRowCount);

        testPlanMatchingPatterns(query,
                new String[]{"Filter\\(condition=\\[=\\<\\($0, 2\\)\\]\\)",
                        "SearchArgument=leaf-0 = \\(LESS_THAN key 2\\), expr = leaf-0"},
                new String[]{});
    }

    @Test
    public void testPushDownWithAndForOrc() throws Exception {
        String query = "select * from hive.orc_push_down where key = 2 and var_key = 'var_6'";

        int actualRowCount = testSql(query);
        assertEquals("Expected and actual row count should match", 1, actualRowCount);

        testPlanMatchingPatterns(query,
                new String[]{"Filter\\(condition=\\[AND\\(=\\($0, 2\\), =\\($2, 'var_6'\\)\\)\\]\\)",
                        "SearchArgument=leaf-0 = \\(EQUALS key 2\\), leaf-1 = \\(EQUALS var_key var_6\\), expr = \\(and leaf-0 leaf-1\\)"},
                new String[]{});
    }

    @Test
    public void testPushDownWithOrForOrc() throws Exception {
        String query = "select * from hive.orc_push_down where key = 2 and var_key = 'var_1'";

        int actualRowCount = testSql(query);
        assertEquals("Expected and actual row count should match", 3, actualRowCount);

        testPlanMatchingPatterns(query,
                new String[]{"Filter\\(condition=\\[OR\\(=\\($0, 2\\), =\\($2, 'var_1'\\)\\)\\]\\)",
                        "SearchArgument=leaf-0 = \\(EQUALS key 2\\), leaf-1 = \\(EQUALS var_key var_1\\), expr = \\(or leaf-0 leaf-1\\)"},
                new String[]{});
    }
}
