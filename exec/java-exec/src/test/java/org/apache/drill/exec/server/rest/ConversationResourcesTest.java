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
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.drill.common.util.JacksonUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.rest.ConversationStore.ConversationRecord;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** HTTP-level tests for the project-scoped conversation endpoints. */
public class ConversationResourcesTest extends ClusterTest {

  private static final MediaType JSON = MediaType.parse("application/json");
  private static final ObjectMapper mapper = JacksonUtils.createObjectMapper();
  private static final OkHttpClient httpClient = new OkHttpClient.Builder()
      .connectTimeout(30, TimeUnit.SECONDS)
      .writeTimeout(30, TimeUnit.SECONDS)
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

  private static String base(String project) {
    return String.format("http://localhost:%d/api/v1/projects/%s/conversations",
        portNumber, project);
  }

  private static ConversationStore store() {
    Drillbit bit = cluster.drillbit();
    return ConversationStore.get(bit.getContext().getStoreProvider(), bit.getManager());
  }

  private static int call(Request.Builder b, JsonNode[] out) throws Exception {
    try (Response response = httpClient.newCall(b.build()).execute()) {
      String body = response.body().string();
      out[0] = body.isEmpty() ? null : mapper.readTree(body);
      return response.code();
    }
  }

  private static int get(String url, JsonNode[] out) throws Exception {
    return call(new Request.Builder().url(url), out);
  }

  private static int post(String url, String json, JsonNode[] out) throws Exception {
    return call(new Request.Builder().url(url).post(RequestBody.create(json, JSON)), out);
  }

  private static int put(String url, String json, JsonNode[] out) throws Exception {
    return call(new Request.Builder().url(url).put(RequestBody.create(json, JSON)), out);
  }

  private static int delete(String url, JsonNode[] out) throws Exception {
    return call(new Request.Builder().url(url).delete(), out);
  }

  /** The principal's name: Drill reports it on the caller's own scratch project. */
  private static String me() throws Exception {
    JsonNode[] out = new JsonNode[1];
    assertEquals(200, get(String.format("http://localhost:%d/api/v1/projects/scratch", portNumber), out));
    return out[0].get("owner").asText();
  }

  private static void seedProject(String id, String owner, boolean isPublic,
      List<String> sharedWith, long deletedAt) {
    Drillbit bit = cluster.drillbit();
    ProjectResources.openStore(bit.getContext().getStoreProvider(), bit.getManager()).put(id,
        new ProjectResources.Project(id, id, "", null, owner, isPublic, sharedWith, null, null,
            null, null, null, false, 1L, 1L, null, null, deletedAt));
  }

  /** A live project the caller owns, so conversations under it are reachable. */
  private static String mine(String id) throws Exception {
    seedProject(id, me(), false, null, 0L);
    return id;
  }

  private static String create(String project) throws Exception {
    JsonNode[] out = new JsonNode[1];
    assertEquals(201, post(base(project), "{\"name\":\"c\"}", out));
    return out[0].get("id").asText();
  }

  private static String messagesBody(String content, int version) throws Exception {
    return "{\"version\":" + version + ",\"messages\":[{\"role\":\"user\",\"content\":"
        + mapper.writeValueAsString(content) + "}]}";
  }

  @Test
  public void postMintsIdAndTakesOwnerFromPrincipal() throws Exception {
    mine("own");
    JsonNode[] out = new JsonNode[1];
    assertEquals(201, post(base("own"), "{\"name\":\"Hello\",\"owner\":\"mallory\"}", out));
    JsonNode rec = out[0];
    assertFalse(rec.get("id").asText().isEmpty());
    assertNotEquals("mallory", rec.get("owner").asText());
    assertEquals("Hello", rec.get("name").asText());
    assertEquals("own", rec.get("projectId").asText());
    assertTrue(rec.get("createdAt").asLong() > 0);

    assertEquals(200, get(base("own"), out));
    assertEquals(1, out[0].get("conversations").size());
  }

  @Test
  public void messagesRoundTripThroughJackson() throws Exception {
    String id = create(mine("rt"));
    JsonNode[] out = new JsonNode[1];
    JsonNode rec;
    assertEquals(200, get(base("rt") + "/" + id, out));
    int version = out[0].get("record").get("version").asInt();

    assertEquals(200, put(base("rt") + "/" + id + "/messages",
        messagesBody("show me the \"top\" rows", version), out));
    assertEquals(version + 1, out[0].get("version").asInt());

    assertEquals(200, get(base("rt") + "/" + id, out));
    assertEquals(1, out[0].get("messages").size());
    assertEquals("show me the \"top\" rows", out[0].get("messages").get(0).get("content").asText());
    assertEquals("user", out[0].get("messages").get(0).get("role").asText());
    assertEquals(1, out[0].get("record").get("messageCount").asInt());

    // Rename keeps the version a client holds valid for its next message write.
    assertEquals(200, put(base("rt") + "/" + id, "{\"name\":\"Renamed\"}", out));
    rec = out[0];
    assertEquals("Renamed", rec.get("name").asText());
    assertEquals(version + 1, rec.get("version").asInt());

    assertEquals(204, delete(base("rt") + "/" + id, out));
    assertEquals(404, get(base("rt") + "/" + id, out));
  }

  @Test
  public void toolCallsAndToolFieldsSurviveRoundTrip() throws Exception {
    String id = create(mine("tc"));
    JsonNode[] out = new JsonNode[1];
    String body = "{\"version\":1,\"messages\":["
        + "{\"role\":\"assistant\",\"content\":\"running\",\"toolCalls\":"
        + "[{\"id\":\"call_1\",\"name\":\"run_sql\",\"arguments\":\"{\\\"sql\\\":\\\"SELECT 1\\\"}\"}]},"
        + "{\"role\":\"tool\",\"content\":\"ok\",\"toolCallId\":\"call_1\",\"name\":\"run_sql\"}]}";
    assertEquals(200, put(base("tc") + "/" + id + "/messages", body, out));

    assertEquals(200, get(base("tc") + "/" + id, out));
    JsonNode msgs = out[0].get("messages");
    assertEquals(2, msgs.size());
    JsonNode call = msgs.get(0).get("toolCalls").get(0);
    assertEquals("call_1", call.get("id").asText());
    assertEquals("run_sql", call.get("name").asText());
    assertEquals("{\"sql\":\"SELECT 1\"}", call.get("arguments").asText());
    assertEquals("call_1", msgs.get(1).get("toolCallId").asText());
    assertEquals("run_sql", msgs.get(1).get("name").asText());
  }

  @Test
  public void renameDoesNotInvalidateTheClientsMessageVersion() throws Exception {
    String id = create(mine("renamever"));
    JsonNode[] out = new JsonNode[1];
    // The client holds the version from creation, before the rename.
    assertEquals(200, get(base("renamever") + "/" + id, out));
    int held = out[0].get("record").get("version").asInt();

    assertEquals(200, put(base("renamever") + "/" + id, "{\"name\":\"Renamed\"}", out));
    assertEquals(200, put(base("renamever") + "/" + id + "/messages",
        messagesBody("after rename", held), out));
  }

  @Test
  public void putMessagesOverCapReturns413() throws Exception {
    String id = create(mine("cap"));
    JsonNode[] out = new JsonNode[1];
    assertEquals(200, put(base("cap") + "/" + id + "/messages", messagesBody("small", 1), out));

    StringBuilder big = new StringBuilder();
    for (int i = 0; i < ConversationStore.MAX_CONVERSATION_BYTES + 10; i++) {
      big.append('x');
    }
    assertEquals(413, put(base("cap") + "/" + id + "/messages", messagesBody(big.toString(), 2), out));
    assertTrue(out[0].get("message").asText().contains(
        String.valueOf(ConversationStore.MAX_CONVERSATION_BYTES)));

    assertEquals(200, get(base("cap") + "/" + id, out));
    assertEquals("small", out[0].get("messages").get(0).get("content").asText());
  }

  @Test
  public void putMessagesWithStaleVersionReturns409() throws Exception {
    String id = create(mine("stale"));
    JsonNode[] out = new JsonNode[1];
    assertEquals(200, put(base("stale") + "/" + id + "/messages", messagesBody("first", 1), out));
    // stored version is now 2; version 1 is stale
    assertEquals(409, put(base("stale") + "/" + id + "/messages", messagesBody("clobber", 1), out));

    assertEquals(200, get(base("stale") + "/" + id, out));
    assertEquals("first", out[0].get("messages").get(0).get("content").asText());
  }

  @Test
  public void nonOwnerGetReturns403() throws Exception {
    mine("own403");
    ConversationRecord r = new ConversationRecord();
    r.setId("alice-conv");
    r.setProjectId("own403");
    r.setOwner("alice");
    r.setName("alice's");
    store().saveRecord(r);

    JsonNode[] out = new JsonNode[1];
    assertEquals(403, get(base("own403") + "/alice-conv", out));
    assertEquals(403, put(base("own403") + "/alice-conv/messages", messagesBody("x", 1), out));
    assertEquals(403, delete(base("own403") + "/alice-conv", out));
    assertEquals(1, store().listForProject("own403", "alice").size());
  }

  @Test
  public void getMissingConversationReturns404() throws Exception {
    mine("missing");
    JsonNode[] out = new JsonNode[1];
    assertEquals(404, get(base("missing") + "/nope", out));
  }

  @Test
  public void aUserWhoCannotReadTheProjectCannotListOrCreateUnderIt() throws Exception {
    // Owned by someone else, private, and the caller is not in sharedWith.
    seedProject("noaccess", "someone-else", false, Arrays.asList("not-me"), 0L);
    JsonNode[] out = new JsonNode[1];

    assertEquals(404, get(base("noaccess"), out));
    assertEquals(404, post(base("noaccess"), "{\"name\":\"x\"}", out));
    // Refused before anything was stored.
    assertEquals(0, store().listForProject("noaccess", me()).size());
  }

  @Test
  public void aUserRemovedFromSharedWithLosesTheirExistingConversations() throws Exception {
    String owner = "someone-else";
    seedProject("shared", owner, false, Arrays.asList(me()), 0L);
    String id = create("shared");
    JsonNode[] out = new JsonNode[1];
    assertEquals(200, get(base("shared") + "/" + id, out));

    seedProject("shared", owner, false, Arrays.asList("not-me"), 0L);

    assertEquals(404, get(base("shared"), out));
    assertEquals(404, get(base("shared") + "/" + id, out));
    assertEquals(404, put(base("shared") + "/" + id + "/messages", messagesBody("x", 1), out));
    assertEquals(404, put(base("shared") + "/" + id, "{\"name\":\"x\"}", out));
    assertEquals(404, delete(base("shared") + "/" + id, out));
    // Nothing was deleted: access lapsed, the conversation did not.
    assertNotNull(store().findRecord(id));
  }

  @Test
  public void aTrashedProjectsConversationsAreNotReachable() throws Exception {
    mine("trashed");
    String id = create("trashed");
    JsonNode[] out = new JsonNode[1];
    assertEquals(200, get(base("trashed") + "/" + id, out));

    // Soft-deleted by its owner, who is the caller: ownership does not override the trash.
    seedProject("trashed", me(), false, null, 12345L);

    assertEquals(404, get(base("trashed"), out));
    assertEquals(404, post(base("trashed"), "{\"name\":\"x\"}", out));
    assertEquals(404, get(base("trashed") + "/" + id, out));
    assertEquals(404, put(base("trashed") + "/" + id + "/messages", messagesBody("x", 1), out));
    assertEquals(404, delete(base("trashed") + "/" + id, out));

    // Restoring the project brings the conversation back untouched.
    seedProject("trashed", me(), false, null, 0L);
    assertEquals(200, get(base("trashed") + "/" + id, out));
  }

  @Test
  public void aMissingProjectReturns404() throws Exception {
    JsonNode[] out = new JsonNode[1];
    assertEquals(404, get(base("never-existed"), out));
    assertEquals(404, post(base("never-existed"), "{\"name\":\"x\"}", out));
  }
}
