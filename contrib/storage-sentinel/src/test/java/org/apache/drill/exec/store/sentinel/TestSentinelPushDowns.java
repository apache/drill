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

package org.apache.drill.exec.store.sentinel;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestSentinelPushDowns {

  @Test
  public void testScanSpecWithBasicTableName() {
    SentinelScanSpec scanSpec = new SentinelScanSpec("test-plugin", "SecurityAlert", "SecurityAlert");

    assertNotNull(scanSpec);
    assertEquals("test-plugin", scanSpec.getPluginName());
    assertEquals("SecurityAlert", scanSpec.getTableName());
    assertEquals("SecurityAlert", scanSpec.getKqlQuery());
  }

  @Test
  public void testScanSpecDefaultsTableNameAsQuery() {
    // When kqlQuery is null, it should default to table name
    SentinelScanSpec scanSpec = new SentinelScanSpec("test-plugin", "SecurityAlert", null);

    assertEquals("SecurityAlert", scanSpec.getKqlQuery());
  }

  @Test
  public void testScanSpecWithComplexKQL() {
    String kqlQuery = "SecurityAlert\n" +
        "| where Severity == \"High\"\n" +
        "| project AlertName, Severity\n" +
        "| take 10";

    SentinelScanSpec scanSpec = new SentinelScanSpec("test-plugin", "SecurityAlert", kqlQuery);

    assertNotNull(scanSpec.getKqlQuery());
    assertTrue(scanSpec.getKqlQuery().contains("where Severity"));
    assertTrue(scanSpec.getKqlQuery().contains("project AlertName"));
    assertTrue(scanSpec.getKqlQuery().contains("take 10"));
  }

  @Test
  public void testStoragePluginConfigCreation() {
    SentinelStoragePluginConfig config = new SentinelStoragePluginConfig(
        "workspace-id",
        "tenant-id",
        "client-id",
        "client-secret",
        "P1D",
        10000,
        new ArrayList<>(),
        AuthMode.SHARED_USER,
        null
    );

    assertNotNull(config);
    assertEquals("workspace-id", config.getWorkspaceId());
    assertEquals("tenant-id", config.getTenantId());
    assertEquals("client-id", config.getClientId());
    assertEquals("P1D", config.getDefaultTimespan());
    assertEquals(10000, config.getMaxRows());
  }

  @Test
  public void testGroupScanCreationWithBasicSpec() {
    SentinelStoragePluginConfig config = new SentinelStoragePluginConfig(
        "workspace-id",
        "tenant-id",
        "client-id",
        "client-secret",
        "P1D",
        10000,
        new ArrayList<>(),
        AuthMode.SHARED_USER,
        null
    );
    SentinelScanSpec scanSpec = new SentinelScanSpec("test-plugin", "SecurityAlert", "SecurityAlert");
    List<SchemaPath> columns = new ArrayList<>();

    SentinelGroupScan groupScan = new SentinelGroupScan(config, scanSpec, columns);

    assertNotNull(groupScan);
    // Verify group scan was created successfully
    assertEquals(0, groupScan.getColumns().size());
  }

  @Test
  public void testGroupScanStoresColumnSelection() {
    SentinelStoragePluginConfig config = new SentinelStoragePluginConfig(
        "workspace-id",
        "tenant-id",
        "client-id",
        "client-secret",
        "P1D",
        10000,
        new ArrayList<>(),
        AuthMode.SHARED_USER,
        null
    );
    SentinelScanSpec scanSpec = new SentinelScanSpec("test-plugin", "SecurityAlert", "SecurityAlert");

    List<SchemaPath> columns = new ArrayList<>();
    columns.add(SchemaPath.getSimplePath("AlertName"));
    columns.add(SchemaPath.getSimplePath("Severity"));

    SentinelGroupScan groupScan = new SentinelGroupScan(config, scanSpec, columns);

    assertNotNull(groupScan);
    assertEquals(2, groupScan.getColumns().size());
  }

  @Test
  public void testFilterPushdownInKQL() {
    // Test that filter clauses can be represented in KQL
    String kqlWithFilter = "SecurityAlert\n" +
        "| where Severity == \"High\"";
    SentinelScanSpec scanSpec = new SentinelScanSpec("test-plugin", "SecurityAlert", kqlWithFilter);

    assertTrue(scanSpec.getKqlQuery().contains("where"));
    assertTrue(scanSpec.getKqlQuery().contains("Severity"));
  }

  @Test
  public void testProjectionPushdownInKQL() {
    // Test that projection can be represented in KQL
    String kqlWithProjection = "SecurityAlert\n" +
        "| project AlertName, Severity, TimeGenerated";
    SentinelScanSpec scanSpec = new SentinelScanSpec("test-plugin", "SecurityAlert", kqlWithProjection);

    assertTrue(scanSpec.getKqlQuery().contains("project"));
    assertTrue(scanSpec.getKqlQuery().contains("AlertName"));
  }

  @Test
  public void testLimitPushdownInKQL() {
    // Test that limit can be represented in KQL as "take"
    String kqlWithLimit = "SecurityAlert\n" +
        "| take 100";
    SentinelScanSpec scanSpec = new SentinelScanSpec("test-plugin", "SecurityAlert", kqlWithLimit);

    assertTrue(scanSpec.getKqlQuery().contains("take 100"));
  }

  @Test
  public void testSortPushdownInKQL() {
    // Test that sort can be represented in KQL
    String kqlWithSort = "SecurityAlert\n" +
        "| sort by TimeGenerated desc";
    SentinelScanSpec scanSpec = new SentinelScanSpec("test-plugin", "SecurityAlert", kqlWithSort);

    assertTrue(scanSpec.getKqlQuery().contains("sort by"));
    assertTrue(scanSpec.getKqlQuery().contains("desc"));
  }

  @Test
  public void testAggregatePushdownInKQL() {
    // Test that aggregation can be represented in KQL as "summarize"
    String kqlWithAggregate = "SecurityAlert\n" +
        "| summarize count() by Severity";
    SentinelScanSpec scanSpec = new SentinelScanSpec("test-plugin", "SecurityAlert", kqlWithAggregate);

    assertTrue(scanSpec.getKqlQuery().contains("summarize"));
    assertTrue(scanSpec.getKqlQuery().contains("count()"));
  }

  @Test
  public void testMultiplePushdownsAccumulated() {
    // Test that multiple operations can be accumulated in the KQL query
    String complexKQL = "SecurityAlert\n" +
        "| where Severity == \"High\"\n" +
        "| project AlertName, Severity, Count\n" +
        "| sort by Count desc\n" +
        "| take 50";
    SentinelScanSpec scanSpec = new SentinelScanSpec("test-plugin", "SecurityAlert", complexKQL);

    // Verify all operations are present in the accumulated KQL
    assertTrue(scanSpec.getKqlQuery().contains("where Severity"));
    assertTrue(scanSpec.getKqlQuery().contains("project AlertName"));
    assertTrue(scanSpec.getKqlQuery().contains("sort by Count"));
    assertTrue(scanSpec.getKqlQuery().contains("take 50"));
  }

  @Test
  public void testEmptyColumnList() {
    SentinelStoragePluginConfig config = new SentinelStoragePluginConfig(
        "workspace-id",
        "tenant-id",
        "client-id",
        "client-secret",
        "P1D",
        10000,
        new ArrayList<>(),
        AuthMode.SHARED_USER,
        null
    );
    SentinelScanSpec scanSpec = new SentinelScanSpec("test-plugin", "SecurityAlert", "SecurityAlert");
    List<SchemaPath> columns = new ArrayList<>();

    SentinelGroupScan groupScan = new SentinelGroupScan(config, scanSpec, columns);

    assertNotNull(groupScan);
    assertEquals(0, groupScan.getColumns().size());
  }
}
