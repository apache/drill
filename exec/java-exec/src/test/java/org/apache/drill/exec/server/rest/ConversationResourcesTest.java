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

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
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
    String id = create("rt");
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
  public void putMessagesOverCapReturns413() throws Exception {
    String id = create("cap");
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
    String id = create("stale");
    JsonNode[] out = new JsonNode[1];
    assertEquals(200, put(base("stale") + "/" + id + "/messages", messagesBody("first", 1), out));
    // stored version is now 2; version 1 is stale
    assertEquals(409, put(base("stale") + "/" + id + "/messages", messagesBody("clobber", 1), out));

    assertEquals(200, get(base("stale") + "/" + id, out));
    assertEquals("first", out[0].get("messages").get(0).get("content").asText());
  }

  @Test
  public void nonOwnerGetReturns403() throws Exception {
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
    JsonNode[] out = new JsonNode[1];
    assertEquals(404, get(base("missing") + "/nope", out));
  }
}
