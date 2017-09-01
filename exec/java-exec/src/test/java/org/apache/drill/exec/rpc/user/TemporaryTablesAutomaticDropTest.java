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
import mockit.integration.junit4.JMockit;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.util.TestUtilities;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(JMockit.class)
public class TemporaryTablesAutomaticDropTest extends BaseTestQuery {

  private static final String session_id = "sessionId";

  private FileSystem fs;

  @Before
  public void setup() throws Exception {
    new MockUp<UUID>() {
      @Mock
      public UUID randomUUID() {
        return UUID.nameUUIDFromBytes(session_id.getBytes());
      }
    };
    Properties testConfigurations = cloneDefaultTestConfigProperties();
    testConfigurations.put(ExecConstants.DEFAULT_TEMPORARY_WORKSPACE, TEMP_SCHEMA);
    updateTestCluster(1, DrillConfig.create(testConfigurations));

    fs = getLocalFileSystem();
  }

  @Test
  public void testAutomaticDropWhenClientIsClosed() throws Exception {
    Path sessionTemporaryLocation = createAndCheckSessionTemporaryLocation("client_closed",
            getDfsTestTmpSchemaLocation());
    updateClient("new_client");
    assertFalse("Session temporary location should be absent", fs.exists(sessionTemporaryLocation));
  }

  @Test
  public void testAutomaticDropWhenDrillbitIsClosed() throws Exception {
    Path sessionTemporaryLocation = createAndCheckSessionTemporaryLocation("drillbit_closed",
            getDfsTestTmpSchemaLocation());
    bits[0].close();
    assertFalse("Session temporary location should be absent", fs.exists(sessionTemporaryLocation));
  }

  @Test
  public void testAutomaticDropOfSeveralSessionTemporaryLocations() throws Exception {
    Path firstSessionTemporaryLocation = createAndCheckSessionTemporaryLocation("first_location",
            getDfsTestTmpSchemaLocation());
    StoragePluginRegistry pluginRegistry = getDrillbitContext().getStorage();
    String tempDir = TestUtilities.createTempDir();
    try {
      TestUtilities.updateDfsTestTmpSchemaLocation(pluginRegistry, tempDir);
      Path secondSessionTemporaryLocation = createAndCheckSessionTemporaryLocation("second_location", tempDir);
      updateClient("new_client");
      assertFalse("First session temporary location should be absent", fs.exists(firstSessionTemporaryLocation));
      assertFalse("Second session temporary location should be absent", fs.exists(secondSessionTemporaryLocation));
    } finally {
      TestUtilities.updateDfsTestTmpSchemaLocation(pluginRegistry, getDfsTestTmpSchemaLocation());
    }
  }

  private Path createAndCheckSessionTemporaryLocation(String suffix, String schemaLocation) throws Exception {
    String temporaryTableName = "temporary_table_automatic_drop_" + suffix;
    test("create TEMPORARY table %s.%s as select 'A' as c1 from (values(1))", TEMP_SCHEMA, temporaryTableName);
    Path sessionTemporaryLocation = new Path(schemaLocation,
            UUID.nameUUIDFromBytes(session_id.getBytes()).toString());
    assertTrue("Session temporary location should exist", fs.exists(sessionTemporaryLocation));
    return sessionTemporaryLocation;
  }

}
