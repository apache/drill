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
  public void testImpersonationDisabled() throws Exception {
    try {
      QueryWrapper q = new QueryWrapper("SHOW SCHEMAS", "SQL", null, "alfred", null);
      runQuery(q);
      fail("Should have thrown exception");
    } catch (UserException e) {
      assertThat(e.getMessage(), containsString("User impersonation is not enabled"));
    }
  }

  @Test
  public void testSpecifyDefaultSchema() throws Exception {
    QueryWrapper.QueryResult result = runQuery(new QueryWrapper("SHOW FILES", "SQL", null, null, "dfs.tmp"));
    // SHOW FILES will fail if default schema is not provided
    assertEquals("COMPLETED", result.queryState);
  }

}
