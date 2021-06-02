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

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.server.rest.RestQueryRunner.QueryResult;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.test.ClusterFixture;
import org.junit.BeforeClass;
import org.junit.Test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public final class TestQueryWrapperImpersonation extends RestServerTest {

  @BeforeClass
  public static void setupServer() {
    startCluster(ClusterFixture.bareBuilder(dirTestWatcher)
      .configProperty(ExecConstants.ALLOW_LOOPBACK_ADDRESS_BINDING, true)
      .configProperty(ExecConstants.IMPERSONATION_ENABLED, true)
      .configProperty(ExecConstants.SEPARATE_WORKSPACE, true));
  }

  @Test
  public void testImpersonation() throws Exception {
    QueryResult result = runQueryWithUsername(
        "SELECT CATALOG_NAME, SCHEMA_NAME FROM information_schema.SCHEMATA", "alfred");
    UserBitShared.QueryProfile queryProfile = getQueryProfile(result);
    assertNotNull(queryProfile);
    assertEquals("alfred", queryProfile.getUser());
  }

  @Test
  public void testImpersonationEnabledButUserNameNotProvided() throws Exception {
    QueryResult result = runQueryWithUsername(
        "SELECT CATALOG_NAME, SCHEMA_NAME FROM information_schema.SCHEMATA", null);
    UserBitShared.QueryProfile queryProfile = getQueryProfile(result);
    assertNotNull(queryProfile);
    assertEquals("anonymous", queryProfile.getUser());
  }

  @Test
  public void testSeparateUserWorkSpace() throws Exception {
    QueryResult result1 = runQueryWithUsername(
            "SELECT CATALOG_NAME, SCHEMA_NAME FROM information_schema.SCHEMATA", "drilluser1");
    UserBitShared.QueryProfile queryProfile1 = getQueryProfile(result1);
    assertNotNull(queryProfile1);
    assertEquals("drilluser1", queryProfile1.getUser());
    QueryResult result2 = runQueryWithUsername(
            "SELECT CATALOG_NAME, SCHEMA_NAME FROM information_schema.SCHEMATA", "drilluser2");
    UserBitShared.QueryProfile queryProfile2 = getQueryProfile(result2);
    assertNotNull(queryProfile2);
    assertEquals("drilluser2", queryProfile2.getUser());

    final DrillUserPrincipal drillUser1PrincipalSeparateWorkspace = new DrillUserPrincipal("drilluser1", true,
            cluster.drillbit().getContext().getConfig().getBoolean(ExecConstants.SEPARATE_WORKSPACE));
    assertFalse("The only drilluser2 can manage his profile",
            drillUser1PrincipalSeparateWorkspace.canManageProfileOf(queryProfile2.getUser()));

    shutdown();
    startCluster(ClusterFixture.bareBuilder(dirTestWatcher)
            .configProperty(ExecConstants.ALLOW_LOOPBACK_ADDRESS_BINDING, true)
            .configProperty(ExecConstants.IMPERSONATION_ENABLED, true)
            .configProperty(ExecConstants.SEPARATE_WORKSPACE, false));
    QueryResult result3 = runQueryWithUsername(
            "SELECT CATALOG_NAME, SCHEMA_NAME FROM information_schema.SCHEMATA", "drilluser3");
    final DrillUserPrincipal drillUser1PrincipalCommonWorkspace = new DrillUserPrincipal("drilluser1", true,
            cluster.drillbit().getContext().getConfig().getBoolean(ExecConstants.SEPARATE_WORKSPACE));
    assertTrue("drilluser1 can manage drilluser3's profile too",
            drillUser1PrincipalCommonWorkspace.canManageProfileOf(getQueryProfile(result3).getUser()));
  }
}
