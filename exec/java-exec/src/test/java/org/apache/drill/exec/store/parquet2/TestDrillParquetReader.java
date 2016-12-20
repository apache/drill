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
package org.apache.drill.exec.store.parquet2;

import org.apache.drill.test.BaseTestQuery;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;

public class TestDrillParquetReader extends BaseTestQuery {
  // enable decimal data type
  @BeforeClass
  public static void enableDecimalDataType() throws Exception {
    test(String.format("alter session set `%s` = true", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }

  @AfterClass
  public static void disableDecimalDataType() throws Exception {
    test(String.format("alter session set `%s` = false", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }

  private void testColumn(String columnName) throws Exception {
    testNoResult("alter session set `store.parquet.use_new_reader` = true");

    BigDecimal result = new BigDecimal("1.20000000");

    testBuilder()
      .ordered()
      .sqlQuery("select %s from cp.`parquet2/decimal28_38.parquet`", columnName)
      .baselineColumns(columnName)
      .baselineValues(result)
      .go();

    testNoResult("alter session set `store.parquet.use_new_reader` = false");
  }

  @Test
  public void testRequiredDecimal28() throws Exception {
    testColumn("d28_req");
  }

  @Test
  public void testRequiredDecimal38() throws Exception {
    testColumn("d38_req");
  }

  @Test
  public void testOptionalDecimal28() throws Exception {
    testColumn("d28_opt");
  }

  @Test
  public void testOptionalDecimal38() throws Exception {
    testColumn("d38_opt");
  }

  @Test
  public void test4349() throws Exception {
    // start by creating a parquet file from the input csv file
    runSQL("CREATE TABLE dfs.tmp.`4349` AS SELECT columns[0] id, CAST(NULLIF(columns[1], '') AS DOUBLE) val FROM cp.`parquet2/4349.csv.gz`");

    // querying the parquet file should return the same results found in the csv file
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT * FROM dfs.tmp.`4349` WHERE id = 'b'")
      .sqlBaselineQuery("SELECT columns[0] id, CAST(NULLIF(columns[1], '') AS DOUBLE) val FROM cp.`parquet2/4349.csv.gz` WHERE columns[0] = 'b'")
      .go();
  }

  @Test
  public void testUnsignedAndSignedIntTypes() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("select * from cp.`parquet/uint_types.parquet`")
      .baselineColumns("uint8_field", "uint16_field", "uint32_field", "uint64_field", "int8_field", "int16_field",
        "required_uint8_field", "required_uint16_field", "required_uint32_field", "required_uint64_field",
        "required_int8_field", "required_int16_field")
      .baselineValues(255, 65535, 2147483647, 9223372036854775807L, 255, 65535, -1, -1, -1, -1L, -2147483648, -2147483648)
      .baselineValues(-1, -1, -1, -1L, -2147483648, -2147483648, 255, 65535, 2147483647, 9223372036854775807L, 255, 65535)
      .baselineValues(null, null, null, null, null, null, 0, 0, 0, 0L, 0, 0)
      .go();
  }
}
