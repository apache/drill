/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.rpc.user;

import mockit.Mock;
import mockit.MockUp;
import org.apache.drill.test.BaseTestQuery;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.util.StoragePluginTestUtils;
import org.apache.drill.test.DirTestWatcher;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Properties;
import java.util.UUID;

import static org.apache.drill.exec.util.StoragePluginTestUtils.DFS_TMP_SCHEMA;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TemporaryTablesAutomaticDropTest extends BaseTestQuery {

  private static final String session_id = "sessionId";

  @Before
  public void setup() throws Exception {
    new MockUp<UUID>() {
      @Mock
      public UUID randomUUID() {
        return UUID.nameUUIDFromBytes(session_id.getBytes());
      }
    };

    Properties testConfigurations = cloneDefaultTestConfigProperties();
    testConfigurations.put(ExecConstants.DEFAULT_TEMPORARY_WORKSPACE, DFS_TMP_SCHEMA);
    updateTestCluster(1, DrillConfig.create(testConfigurations));
  }

  @Test
  public void testAutomaticDropWhenClientIsClosed() throws Exception {
    final File sessionTemporaryLocation =
      createAndCheckSessionTemporaryLocation("client_closed", dirTestWatcher.getDfsTestTmpDir());

    updateClient("new_client");
    assertFalse("Session temporary location should be absent", sessionTemporaryLocation.exists());
  }

  @Test
  public void testAutomaticDropWhenDrillbitIsClosed() throws Exception {
    final File sessionTemporaryLocation =
      createAndCheckSessionTemporaryLocation("drillbit_closed", dirTestWatcher.getDfsTestTmpDir());
    bits[0].close();
    assertFalse("Session temporary location should be absent", sessionTemporaryLocation.exists());
  }

  @Test
  public void testAutomaticDropOfSeveralSessionTemporaryLocations() throws Exception {
    final File firstSessionTemporaryLocation =
      createAndCheckSessionTemporaryLocation("first_location", dirTestWatcher.getDfsTestTmpDir());
    final StoragePluginRegistry pluginRegistry = getDrillbitContext().getStorage();
    final File tempDir = DirTestWatcher.createTempDir(dirTestWatcher.getDir());

    try {
      StoragePluginTestUtils.updateSchemaLocation(StoragePluginTestUtils.DFS_PLUGIN_NAME, pluginRegistry, tempDir);
      final File secondSessionTemporaryLocation = createAndCheckSessionTemporaryLocation("second_location", tempDir);
      updateClient("new_client");
      assertFalse("First session temporary location should be absent", firstSessionTemporaryLocation.exists());
      assertFalse("Second session temporary location should be absent", secondSessionTemporaryLocation.exists());
    } finally {
      StoragePluginTestUtils.updateSchemaLocation(StoragePluginTestUtils.DFS_PLUGIN_NAME, pluginRegistry, dirTestWatcher.getDfsTestTmpDir());
    }
  }

  private File createAndCheckSessionTemporaryLocation(String suffix, File schemaLocation) throws Exception {
    final String temporaryTableName = "temporary_table_automatic_drop_" + suffix;
    final File sessionTemporaryLocation = schemaLocation
      .toPath()
      .resolve(UUID.nameUUIDFromBytes(session_id.getBytes()).toString())
      .toFile();

    test("create TEMPORARY table %s.%s as select 'A' as c1 from (values(1))", DFS_TMP_SCHEMA, temporaryTableName);
    assertTrue("Session temporary location should exist", sessionTemporaryLocation.exists());
    return sessionTemporaryLocation;
  }
}
