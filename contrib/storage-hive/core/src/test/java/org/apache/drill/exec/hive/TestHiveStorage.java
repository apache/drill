/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.hive;

import com.google.common.collect.ImmutableMap;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.hadoop.fs.FileSystem;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class TestHiveStorage extends HiveTestBase {
  @BeforeClass
  public static void setupOptions() throws Exception {
    test(String.format("alter session set `%s` = true", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }


  @Test // DRILL-4083
  public void testNativeScanWhenNoColumnIsRead() throws Exception {
    try {
      test(String.format("alter session set `%s` = true", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));

      String query = "SELECT count(*) as col FROM hive.countStar_Parquet";
      testPhysicalPlan(query, "hive-drill-native-parquet-scan");

      testBuilder()
          .sqlQuery(query)
          .unOrdered()
          .baselineColumns("col")
          .baselineValues(200l)
          .go();
    } finally {
      test(String.format("alter session set `%s` = %s",
          ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS,
              ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS_VALIDATOR.getDefault().bool_val ? "true" : "false"));
    }
  }

  @Test
  public void hiveReadWithDb() throws Exception {
    test("select * from hive.kv");
  }

  @Test
  public void queryEmptyHiveTable() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM hive.empty_table")
        .expectsEmptyResultSet()
        .go();
  }

  @Test // DRILL-3328
  public void convertFromOnHiveBinaryType() throws Exception {
    testBuilder()
        .sqlQuery("SELECT convert_from(binary_field, 'UTF8') col1 from hive.readtest")
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues("binaryfield")
        .baselineValues(new Object[]{null})
        .go();
  }

  /**
   * Test to ensure Drill reads the all supported types correctly both normal fields (converted to Nullable types) and
   * partition fields (converted to Required types).
   * @throws Exception
   */
  @Test
  public void readAllSupportedHiveDataTypes() throws Exception {
    testBuilder().sqlQuery("SELECT * FROM hive.readtest")
        .unOrdered()
        .baselineColumns(
            "binary_field",
            "boolean_field",
            "tinyint_field",
            "decimal0_field",
            "decimal9_field",
            "decimal18_field",
            "decimal28_field",
            "decimal38_field",
            "double_field",
            "float_field",
            "int_field",
            "bigint_field",
            "smallint_field",
            "string_field",
            "varchar_field",
            "timestamp_field",
            "date_field",
            "char_field",
            // There is a regression in Hive 1.2.1 in binary type partition columns. Disable for now.
            //"binary_part",
            "boolean_part",
            "tinyint_part",
            "decimal0_part",
            "decimal9_part",
            "decimal18_part",
            "decimal28_part",
            "decimal38_part",
            "double_part",
            "float_part",
            "int_part",
            "bigint_part",
            "smallint_part",
            "string_part",
            "varchar_part",
            "timestamp_part",
            "date_part",
            "char_part")
        .baselineValues(
            "binaryfield".getBytes(),
            false,
            34,
            new BigDecimal("66"),
            new BigDecimal("2347.92"),
            new BigDecimal("2758725827.99990"),
            new BigDecimal("29375892739852.8"),
            new BigDecimal("89853749534593985.783"),
            8.345d,
            4.67f,
            123456,
            234235L,
            3455,
            "stringfield",
            "varcharfield",
            new DateTime(Timestamp.valueOf("2013-07-05 17:01:00").getTime()),
            new DateTime(Date.valueOf("2013-07-05").getTime()),
            "charfield",
            // There is a regression in Hive 1.2.1 in binary type partition columns. Disable for now.
            //"binary",
            true,
            64,
            new BigDecimal("37"),
            new BigDecimal("36.90"),
            new BigDecimal("3289379872.94565"),
            new BigDecimal("39579334534534.4"),
            new BigDecimal("363945093845093890.900"),
            8.345d,
            4.67f,
            123456,
            234235L,
            3455,
            "string",
            "varchar",
            new DateTime(Timestamp.valueOf("2013-07-05 17:01:00").getTime()),
            new DateTime(Date.valueOf("2013-07-05").getTime()),
            "char")
        .baselineValues( // All fields are null, but partition fields have non-null values
            null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
            // There is a regression in Hive 1.2.1 in binary type partition columns. Disable for now.
            //"binary",
            true,
            64,
            new BigDecimal("37"),
            new BigDecimal("36.90"),
            new BigDecimal("3289379872.94565"),
            new BigDecimal("39579334534534.4"),
            new BigDecimal("363945093845093890.900"),
            8.345d,
            4.67f,
            123456,
            234235L,
            3455,
            "string",
            "varchar",
            new DateTime(Timestamp.valueOf("2013-07-05 17:01:00").getTime()),
            new DateTime(Date.valueOf("2013-07-05").getTime()),
            "char")
        .go();
  }

  /**
   * Test to ensure Drill reads the all supported types through native Parquet readers.
   * NOTE: As part of Hive 1.2 upgrade, make sure this test and {@link #readAllSupportedHiveDataTypes()} are merged
   * into one test.
   */
  @Test
  public void readAllSupportedHiveDataTypesNativeParquet() throws Exception {
    try {
      test(String.format("alter session set `%s` = true", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
      final String query = "SELECT * FROM hive.readtest_parquet";

      // Make sure the plan has Hive scan with native parquet reader
      testPhysicalPlan(query, "hive-drill-native-parquet-scan");

      testBuilder().sqlQuery(query)
          .unOrdered()
          .baselineColumns(
              "binary_field",
              "boolean_field",
              "tinyint_field",
              "decimal0_field",
              "decimal9_field",
              "decimal18_field",
              "decimal28_field",
              "decimal38_field",
              "double_field",
              "float_field",
              "int_field",
              "bigint_field",
              "smallint_field",
              "string_field",
              "varchar_field",
              "timestamp_field",
              "char_field",
              // There is a regression in Hive 1.2.1 in binary and boolean partition columns. Disable for now.
              //"binary_part",
              "boolean_part",
              "tinyint_part",
              "decimal0_part",
              "decimal9_part",
              "decimal18_part",
              "decimal28_part",
              "decimal38_part",
              "double_part",
              "float_part",
              "int_part",
              "bigint_part",
              "smallint_part",
              "string_part",
              "varchar_part",
              "timestamp_part",
              "date_part",
              "char_part")
          .baselineValues(
              "binaryfield".getBytes(),
              false,
              34,
              new BigDecimal("66"),
              new BigDecimal("2347.92"),
              new BigDecimal("2758725827.99990"),
              new BigDecimal("29375892739852.8"),
              new BigDecimal("89853749534593985.783"),
              8.345d,
              4.67f,
              123456,
              234235L,
              3455,
              "stringfield",
              "varcharfield",
              new DateTime(Timestamp.valueOf("2013-07-05 17:01:00").getTime()),
              "charfield",
              // There is a regression in Hive 1.2.1 in binary and boolean partition columns. Disable for now.
              //"binary",
              true,
              64,
              new BigDecimal("37"),
              new BigDecimal("36.90"),
              new BigDecimal("3289379872.94565"),
              new BigDecimal("39579334534534.4"),
              new BigDecimal("363945093845093890.900"),
              8.345d,
              4.67f,
              123456,
              234235L,
              3455,
              "string",
              "varchar",
              new DateTime(Timestamp.valueOf("2013-07-05 17:01:00").getTime()),
              new DateTime(Date.valueOf("2013-07-05").getTime()),
              "char")
          .baselineValues( // All fields are null, but partition fields have non-null values
              null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
              // There is a regression in Hive 1.2.1 in binary and boolean partition columns. Disable for now.
              //"binary",
              true,
              64,
              new BigDecimal("37"),
              new BigDecimal("36.90"),
              new BigDecimal("3289379872.94565"),
              new BigDecimal("39579334534534.4"),
              new BigDecimal("363945093845093890.900"),
              8.345d,
              4.67f,
              123456,
              234235L,
              3455,
              "string",
              "varchar",
              new DateTime(Timestamp.valueOf("2013-07-05 17:01:00").getTime()),
              new DateTime(Date.valueOf("2013-07-05").getTime()),
              "char")
          .go();
    } finally {
        test(String.format("alter session set `%s` = false", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
    }
  }

  @Test
  public void orderByOnHiveTable() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM hive.kv ORDER BY `value` DESC")
        .ordered()
        .baselineColumns("key", "value")
        .baselineValues(5, " key_5")
        .baselineValues(4, " key_4")
        .baselineValues(3, " key_3")
        .baselineValues(2, " key_2")
        .baselineValues(1, " key_1")
        .go();
  }

  @Test
  public void queryingTablesInNonDefaultFS() throws Exception {
    // Update the default FS settings in Hive test storage plugin to non-local FS
    hiveTest.updatePluginConfig(getDrillbitContext().getStorage(),
        ImmutableMap.of(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://localhost:9001"));

    testBuilder()
        .sqlQuery("SELECT * FROM hive.`default`.kv LIMIT 1")
        .unOrdered()
        .baselineColumns("key", "value")
        .baselineValues(1, " key_1")
        .go();
  }

  @Test // DRILL-745
  public void queryingHiveAvroTable() throws Exception {
      testBuilder()
          .sqlQuery("SELECT * FROM hive.db1.avro ORDER BY key DESC LIMIT 1")
        .unOrdered()
        .baselineColumns("key", "value")
        .baselineValues(5, " key_5")
        .go();
  }

  @Test // DRILL-3266
  public void queryingTableWithSerDeInHiveContribJar() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM hive.db1.kv_db1 ORDER BY key DESC LIMIT 1")
        .unOrdered()
        .baselineColumns("key", "value")
        .baselineValues("5", " key_5")
        .go();
  }


  @Test // DRILL-3746
  public void readFromPartitionWithCustomLocation() throws Exception {
    testBuilder()
        .sqlQuery("SELECT count(*) as cnt FROM hive.partition_pruning_test WHERE c=99 AND d=98 AND e=97")
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(1L)
        .go();
  }

  @Test // DRILL-3938
  public void readFromAlteredPartitionedTable() throws Exception {
    testBuilder()
        .sqlQuery("SELECT key, `value`, newcol FROM hive.kv_parquet ORDER BY key LIMIT 1")
        .unOrdered()
        .baselineColumns("key", "value", "newcol")
        .baselineValues(1, " key_1", null)
        .go();
  }

  @Test // DRILL-3938
  public void nativeReaderIsDisabledForAlteredPartitionedTable() throws Exception {
    try {
      test(String.format("alter session set `%s` = true", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
      final String query = "EXPLAIN PLAN FOR SELECT key, `value`, newcol FROM hive.kv_parquet ORDER BY key LIMIT 1";

      // Make sure the HiveScan in plan has no native parquet reader
      final String planStr = getPlanInString(query, JSON_FORMAT);
      assertFalse("Hive native is not expected in the plan", planStr.contains("hive-drill-native-parquet-scan"));
    } finally {
      test(String.format("alter session set `%s` = false", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
    }
  }

  @Test // DRILL-3739
  public void readingFromStorageHandleBasedTable() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM hive.kv_sh ORDER BY key LIMIT 2")
        .ordered()
        .baselineColumns("key", "value")
        .expectsEmptyResultSet()
        .go();
  }

  @Test // DRILL-3739
  public void readingFromStorageHandleBasedTable2() throws Exception {
    try {
      test(String.format("alter session set `%s` = true", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));

      testBuilder()
          .sqlQuery("SELECT * FROM hive.kv_sh ORDER BY key LIMIT 2")
          .ordered()
          .baselineColumns("key", "value")
          .expectsEmptyResultSet()
          .go();
    } finally {
      test(String.format("alter session set `%s` = false", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
    }
  }

  @Test // DRILL-3688
  public void readingFromSmallTableWithSkipHeaderAndFooter() throws Exception {
   testBuilder()
        .sqlQuery("select key, `value` from hive.skipper.kv_text_small order by key asc")
        .ordered()
        .baselineColumns("key", "value")
        .baselineValues(1, "key_1")
        .baselineValues(2, "key_2")
        .baselineValues(3, "key_3")
        .baselineValues(4, "key_4")
        .baselineValues(5, "key_5")
        .go();

    testBuilder()
        .sqlQuery("select count(1) as cnt from hive.skipper.kv_text_small")
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(5L)
        .go();
  }

  @Test // DRILL-3688
  public void readingFromLargeTableWithSkipHeaderAndFooter() throws Exception {
    testBuilder()
        .sqlQuery("select sum(key) as sum_keys from hive.skipper.kv_text_large")
        .unOrdered()
        .baselineColumns("sum_keys")
        .baselineValues((long)(5000*(5000 + 1)/2))
        .go();

    testBuilder()
        .sqlQuery("select count(1) as cnt from hive.skipper.kv_text_large")
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(5000L)
        .go();
  }

  @Test // DRILL-3688
  public void testIncorrectHeaderFooterProperty() throws Exception {
    Map<String, String> testData = ImmutableMap.<String, String>builder()
        .put("hive.skipper.kv_incorrect_skip_header","skip.header.line.count")
        .put("hive.skipper.kv_incorrect_skip_footer", "skip.footer.line.count")
        .build();

    String query = "select * from %s";
    String exceptionMessage = "Hive table property %s value 'A' is non-numeric";

    for (Map.Entry<String, String> entry : testData.entrySet()) {
      try {
        test(String.format(query, entry.getKey()));
      } catch (UserRemoteException e) {
        assertThat(e.getMessage(), containsString(String.format(exceptionMessage, entry.getValue())));
      }
    }
  }

  @Test // DRILL-3688
  public void testIgnoreSkipHeaderFooterForRcfile() throws Exception {
    testBuilder()
        .sqlQuery("select count(1) as cnt from hive.skipper.kv_rcfile_large")
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(5000L)
        .go();
  }

  @Test // DRILL-3688
  public void testIgnoreSkipHeaderFooterForParquet() throws Exception {
    testBuilder()
        .sqlQuery("select count(1) as cnt from hive.skipper.kv_parquet_large")
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(5000L)
        .go();
  }

  @Test // DRILL-3688
  public void testIgnoreSkipHeaderFooterForSequencefile() throws Exception {
    testBuilder()
        .sqlQuery("select count(1) as cnt from hive.skipper.kv_sequencefile_large")
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(5000L)
        .go();
  }

  @AfterClass
  public static void shutdownOptions() throws Exception {
    test(String.format("alter session set `%s` = false", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }
}
