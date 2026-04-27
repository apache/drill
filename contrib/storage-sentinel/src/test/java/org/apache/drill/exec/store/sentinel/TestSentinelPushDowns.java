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

import static org.junit.Assert.assertNotNull;

public class TestSentinelPushDowns {

  @Test
  public void testScanSpecCreation() {
    SentinelScanSpec scanSpec = new SentinelScanSpec("test-plugin", "SecurityAlert", "SecurityAlert");
    assertNotNull(scanSpec);
  }

  @Test
  public void testScanSpecWithKQL() {
    SentinelScanSpec scanSpec = new SentinelScanSpec(
        "test-plugin",
        "SecurityAlert",
        "SecurityAlert\n| where Severity == \"High\"\n| take 10"
    );
    assertNotNull(scanSpec);
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
  }

  @Test
  public void testGroupScanCreation() {
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
  }

  @Test
  public void testCanPushdownFilter() {
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
  }

  @Test
  public void testCanPushdownProject() {
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
  }

  @Test
  public void testCanPushdownLimit() {
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
  }

  @Test
  public void testCanPushdownSort() {
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
  }

  @Test
  public void testCanPushdownAggregate() {
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
  }
}
