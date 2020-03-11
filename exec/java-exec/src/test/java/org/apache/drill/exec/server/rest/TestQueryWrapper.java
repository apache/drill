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
package org.apache.drill.exec.server.rest;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.ClusterFixture;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.drill.exec.ExecConstants.ENABLE_VERBOSE_ERRORS_KEY;
import static org.apache.drill.exec.ExecConstants.OUTPUT_FORMAT_OPTION;
import static org.apache.drill.exec.ExecConstants.PARQUET_FLAT_BATCH_MEMORY_SIZE_VALIDATOR;
import static org.apache.drill.exec.ExecConstants.QUERY_MAX_ROWS;
import static org.apache.drill.exec.ExecConstants.TEXT_ESTIMATED_ROW_SIZE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class TestQueryWrapper extends RestServerTest {

  @BeforeClass
  public static void setupServer() throws Exception {
    startCluster(ClusterFixture.bareBuilder(dirTestWatcher)
      .clusterSize(1)
      .configProperty(ExecConstants.ALLOW_LOOPBACK_ADDRESS_BINDING, true));
  }

  @Test
  public void testShowSchemas() throws Exception {
    QueryWrapper.QueryResult result = runQuery("SHOW SCHEMAS");
    assertEquals("COMPLETED", result.queryState);
    assertNotEquals(0, result.rows.size());
    assertEquals(1, result.columns.size());
    assertEquals(result.columns.iterator().next(), "SCHEMA_NAME");
  }

  @Test
  public void testSpecifyOutputFormatAsParquet() throws Exception {
    QueryWrapper.QueryResult result = runQueryWithOption(
      "CREATE TABLE dfs.tmp.schemata_parquet AS SELECT CATALOG_NAME, SCHEMA_NAME FROM information_schema.SCHEMATA",
      OUTPUT_FORMAT_OPTION,
      "parquet");
    assertEquals("COMPLETED", result.queryState);
    assertEquals(2, result.columns.size());
    assertEquals(2, result.metadata.size());
    assertEquals(1, result.rows.size());
    assertEquals("0_0", result.rows.get(0).get("Fragment"));
    assertNotEquals("0", result.rows.get(0).get("Number of records written"));
    QueryWrapper.QueryResult result2 = runQuery("SHOW FILES IN dfs.tmp.schemata_parquet");
    assertEquals("0_0_0.parquet", result2.rows.get(0).get("name"));
  }

  @Test
  public void testSpecifyOutputFormatAsJson() throws Exception {
    QueryWrapper.QueryResult result = runQueryWithOption(
      "CREATE TABLE dfs.tmp.schemata_json AS SELECT CATALOG_NAME, SCHEMA_NAME FROM information_schema.SCHEMATA",
      OUTPUT_FORMAT_OPTION,
      "json");
    assertEquals("COMPLETED", result.queryState);
    assertEquals(2, result.columns.size());
    assertEquals(2, result.metadata.size());
    assertEquals(1, result.rows.size());
    assertEquals("0_0", result.rows.get(0).get("Fragment"));
    assertNotEquals("0", result.rows.get(0).get("Number of records written"));
    QueryWrapper.QueryResult result2 = runQuery("SHOW FILES IN dfs.tmp.schemata_json");
    assertEquals("0_0_0.json", result2.rows.get(0).get("name"));
  }

  @Test
  public void testInvalidOptionName() throws Exception {
    try {
      runQueryWithOption("SHOW SCHEMAS", "xxx", "s");
      fail("Expected exception to be thrown");
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString("The option 'xxx' does not exist."));
    }
  }

  @Test
  public void testBooleanOptionGivenAsString() {
    try {
      runQueryWithOption("SHOW SCHEMAS", ENABLE_VERBOSE_ERRORS_KEY, "not a boolean");
      fail("Expected exception to be thrown");
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString("Expected boolean value"));
    }
  }

  @Test
  public void testBooleanOptionGivenAsNumber() {
    try {
      runQueryWithOption("SHOW SCHEMAS", ENABLE_VERBOSE_ERRORS_KEY, Long.valueOf(7));
      fail("Expected exception to be thrown");
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString("Expected boolean value"));
    }
  }

  @Test
  public void testStringOptionGivenAsBoolean() {
    try {
      runQueryWithOption("SHOW SCHEMAS", OUTPUT_FORMAT_OPTION, Boolean.TRUE);
      fail("Expected exception to be thrown");
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString("Expected string value"));
    }
  }

  @Test
  public void testStringOptionGivenAsNumber() {
    try {
      runQueryWithOption("SHOW SCHEMAS", OUTPUT_FORMAT_OPTION, Long.valueOf(7));
      fail("Expected exception to be thrown");
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString("Expected string value"));
    }
  }

  @Test
  public void testDoubleOptionGivenAsString() {
    try {
      runQueryWithOption("SHOW SCHEMAS", TEXT_ESTIMATED_ROW_SIZE.getOptionName(), "3.14");
      fail("Expected exception to be thrown");
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString("Expected number value"));
    }
  }

  @Test
  public void testLongOptionGivenAsString() {
    try {
      runQueryWithOption("SHOW SCHEMAS", QUERY_MAX_ROWS, "3.14");
      fail("Expected exception to be thrown");
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString("Expected number value"));
    }
  }

  @Test
  public void testInternalOptionGiven() {
    try {
      runQueryWithOption("SHOW SCHEMAS", PARQUET_FLAT_BATCH_MEMORY_SIZE_VALIDATOR.getOptionName(), "3.14");
      fail("Expected exception to be thrown");
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString("Internal option 'store.parquet.flat.batch.memory_size' cannot be set with query"));
    }
  }

  @Test
  public void testImpersonationDisabled() throws Exception {
    try {
      QueryWrapper q = new QueryWrapper("SHOW SCHEMAS", "SQL", null, "alfred", null, null);
      runQuery(q);
      fail("Should have thrown exception");
    } catch (UserException e) {
      assertThat(e.getMessage(), containsString("User impersonation is not enabled"));
    }
  }

  @Test
  public void testSpecifyDefaultSchema() throws Exception {
    QueryWrapper.QueryResult result = runQuery(new QueryWrapper("SHOW FILES", "SQL", null, null, "dfs.tmp", null));
    // SHOW FILES will fail if default schema is not provided
    assertEquals("COMPLETED", result.queryState);
  }

}
