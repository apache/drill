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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.drill.common.util.JacksonUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.rest.ProjectResources.Project;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/** Tests for the per-user Scratch project used by conversations outside any project. */
public class ScratchProjectTest extends ClusterTest {

  private static final ObjectMapper mapper = JacksonUtils.createObjectMapper();
  private static final OkHttpClient httpClient = new OkHttpClient.Builder()
      .connectTimeout(30, TimeUnit.SECONDS)
      .readTimeout(30, TimeUnit.SECONDS)
      .build();
  private static int portNumber;

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher)
        .configProperty(ExecConstants.HTTP_ENABLE, true)
        .configProperty(ExecConstants.HTTP_PORT_HUNT, true);
    startCluster(builder);
    portNumber = cluster.drillbit().getWebServerPort();
  }

  private static int get(String path, JsonNode[] out) throws Exception {
    Request req = new Request.Builder()
        .url(String.format("http://localhost:%d/api/v1/projects%s", portNumber, path)).build();
    try (Response response = httpClient.newCall(req).execute()) {
      String body = response.body().string();
      out[0] = body.isEmpty() ? null : mapper.readTree(body);
      return response.code();
    }
  }

  private static PersistentStore<Project> store() {
    Drillbit bit = cluster.drillbit();
    return ProjectResources.openStore(bit.getContext().getStoreProvider(), bit.getManager());
  }

  @Test
  public void scratchIsCreatedOnFirstCallAndReusedOnSecond() throws Exception {
    JsonNode[] out = new JsonNode[1];
    assertEquals(200, get("/scratch", out));
    JsonNode first = out[0];
    assertTrue(first.get("id").asText().startsWith("scratch_"));
    assertEquals("Scratch", first.get("name").asText());
    assertTrue(first.get("isSystem").asBoolean());
    assertFalse(first.get("isPublic").asBoolean());

    assertEquals(200, get("/scratch", out));
    assertEquals(first.get("id").asText(), out[0].get("id").asText());
    assertEquals(first.get("createdAt").asLong(), out[0].get("createdAt").asLong());

    // Stored exactly once under that id, and the id is usable as a project id.
    assertEquals(first.get("id").asText(), store().get(first.get("id").asText()).getId());
    assertEquals(200, get("/" + first.get("id").asText(), out));
  }

  @Test
  public void scratchIsPerUser() {
    Project alice = ProjectResources.getOrCreateScratch(store(), "alice");
    Project aliceAgain = ProjectResources.getOrCreateScratch(store(), "alice");
    Project bob = ProjectResources.getOrCreateScratch(store(), "bob");
    assertEquals("scratch_alice", alice.getId());
    assertEquals(alice.getId(), aliceAgain.getId());
    assertEquals(alice.getCreatedAt(), aliceAgain.getCreatedAt());
    assertNotEquals(alice.getId(), bob.getId());
    assertEquals("alice", alice.getOwner());
    assertEquals("bob", bob.getOwner());
  }

  @Test
  public void scratchIsNotListedAmongOrdinaryProjects() throws Exception {
    JsonNode[] out = new JsonNode[1];
    assertEquals(200, get("/scratch", out));
    String scratchId = out[0].get("id").asText();
    assertTrue(store().get(scratchId) != null);

    assertEquals(200, get("", out));
    for (JsonNode p : out[0].get("projects")) {
      assertNotEquals(scratchId, p.get("id").asText());
    }
  }
}
