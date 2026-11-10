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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Handler-level checks that destructive project endpoints stay owner-only. The harness
 * issues every request as the anonymous user, so each project is seeded with another
 * owner and the requester as an editor ({@code sharedWith}). Reverting any destructive
 * handler to {@code canWrite} turns the 403 below into a success.
 */
public class TestProjectResourcesAccess extends ClusterTest {

  private static final MediaType JSON = MediaType.parse("application/json");
  private static int portNumber;
  private static final OkHttpClient client = new OkHttpClient.Builder()
      .connectTimeout(30, TimeUnit.SECONDS)
      .readTimeout(30, TimeUnit.SECONDS)
      .build();

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(new ClusterFixtureBuilder(dirTestWatcher)
        .configProperty(ExecConstants.HTTP_ENABLE, true)
        .configProperty(ExecConstants.HTTP_PORT_HUNT, true));
    portNumber = cluster.drillbit().getWebServerPort();
  }

  /** Seeds a project owned by someone else with the requester as an editor. */
  private static String seed(long deletedAt) {
    String id = UUID.randomUUID().toString();
    ProjectResources.Project p = new ProjectResources.Project(
        id, "Original", null, null, "someone-else", false,
        Arrays.asList(DrillUserPrincipal.ANONYMOUS_USER), Collections.<String>emptyList(),
        null, null, null, null, null, false, 1L, 1L, null, null, deletedAt);
    ProjectResources.openStore(cluster.drillbit().getContext().getStoreProvider(),
        cluster.drillbit().getManager()).put(id, p);
    return id;
  }

  private static int call(String method, String path, String json) throws Exception {
    Request.Builder b = new Request.Builder()
        .url("http://localhost:" + portNumber + "/api/v1/projects" + path);
    RequestBody body = RequestBody.create(json == null ? "" : json, JSON);
    switch (method) {
      case "DELETE":
        b.delete();
        break;
      case "PUT":
        b.put(body);
        break;
      default:
        b.post(body);
    }
    try (Response r = client.newCall(b.build()).execute()) {
      return r.code();
    }
  }

  @Test
  public void testOnlyOwnerCanDelete() throws Exception {
    assertEquals(403, call("DELETE", "/" + seed(0L), null));
  }

  @Test
  public void testOnlyOwnerCanRestore() throws Exception {
    assertEquals(403, call("POST", "/" + seed(5L) + "/restore", null));
  }

  @Test
  public void testOnlyOwnerCanPurge() throws Exception {
    assertEquals(403, call("DELETE", "/" + seed(5L) + "/purge", null));
  }

  @Test
  public void testEditorCannotChangeSharing() throws Exception {
    assertEquals(403, call("PUT", "/" + seed(0L), "{\"isPublic\":true}"));
  }

  @Test
  public void testEditorCanChangeContentFields() throws Exception {
    assertEquals(200, call("PUT", "/" + seed(0L), "{\"name\":\"Renamed\"}"));
  }
}
