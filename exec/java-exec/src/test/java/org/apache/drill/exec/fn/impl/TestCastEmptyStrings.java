/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.fn.impl;

import org.apache.drill.test.BaseTestQuery;
import org.apache.drill.categories.SqlFunctionTest;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({UnlikelyTest.class, SqlFunctionTest.class})
public class TestCastEmptyStrings extends BaseTestQuery {
  // enable decimal data type
  @BeforeClass
  public static void enableDecimalDataType() throws Exception {
    test("alter session set `%s` = true", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY);
  }

  @AfterClass
  public static void disableDecimalDataType() throws Exception {
    test("alter session set `%s` = false", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY);
  }

  @Test // see DRILL-1874
  public void testCastInputTypeNullableVarCharToNumeric() throws Exception {
    // Enable the new cast functions (cast empty string "" to null)
    test("alter system set `drill.exec.functions.cast_empty_string_to_null` = true;");

    // Test Optional VarChar
    test("select cast(columns[0] as int) from cp.`emptyStrings.csv`");
    test("select cast(columns[0] as bigint) from cp.`emptyStrings.csv`");
    test("select cast(columns[0] as float) from cp.`emptyStrings.csv`");
    test("select cast(columns[0] as double) from cp.`emptyStrings.csv`");
    test("alter system set `drill.exec.functions.cast_empty_string_to_null` = false;");
  }

  @Test // see DRILL-1874
  public void testCastInputTypeNonNullableVarCharToNumeric() throws Exception {
    // Enable the new cast functions (cast empty string "" to null)
    test("alter system set `drill.exec.functions.cast_empty_string_to_null` = true;");
    // Test Required VarChar
    test("select cast('' as int) from cp.`emptyStrings.csv`");
    test("select cast('' as bigint) from cp.`emptyStrings.csv`");
    test("select cast('' as float) from cp.`emptyStrings.csv`");
    test("select cast('' as double) from cp.`emptyStrings.csv`");
    test("alter system set `drill.exec.functions.cast_empty_string_to_null` = false;");
  }

  @Test // see DRILL-1874
  public void testCastInputTypeNullableVarCharToDecimal() throws Exception {
    // Enable the new cast functions (cast empty string "" to null)
    test("alter system set `drill.exec.functions.cast_empty_string_to_null` = true;");

    // Test Optional VarChar
    test("select cast(columns[0] as decimal) from cp.`emptyStrings.csv` where cast(columns[0] as decimal) is null");
    test("select cast(columns[0] as decimal(9)) from cp.`emptyStrings.csv`");
    test("select cast(columns[0] as decimal(18)) from cp.`emptyStrings.csv`");
    test("select cast(columns[0] as decimal(28)) from cp.`emptyStrings.csv`");
    test("select cast(columns[0] as decimal(38)) from cp.`emptyStrings.csv`");

    test("alter system set `drill.exec.functions.cast_empty_string_to_null` = false;");
  }

  @Test // see DRILL-1874
  public void testCastInputTypeNonNullableVarCharToDecimal() throws Exception {
    // Enable the new cast functions (cast empty string "" to null)
    test("alter system set `drill.exec.functions.cast_empty_string_to_null` = true;");

    // Test Required VarChar
    test("select cast('' as decimal) from cp.`emptyStrings.csv` where cast('' as decimal) is null");
    test("select cast('' as decimal(18)) from cp.`emptyStrings.csv`");
    test("select cast('' as decimal(28)) from cp.`emptyStrings.csv`");
    test("select cast('' as decimal(38)) from cp.`emptyStrings.csv`");

    test("alter system set `drill.exec.functions.cast_empty_string_to_null` = false;");
  }
}
