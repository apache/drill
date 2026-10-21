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
import org.apache.drill.common.util.JacksonUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Per-tab Prospector conversations.
 *
 * <p>Conversations live in their own store rather than on the tab record: they are far
 * larger than a tab, and the project tree reads the tab listing on every expand.
 *
 * <p>The size cap is not arbitrary. {@code PersistentStore} defaults to
 * {@code ZookeeperPersistentStoreProvider} (drill-module.conf), whose write path is
 * {@code client.put(key, bytes)} straight into a znode. ZooKeeper's default
 * {@code jute.maxbuffer} is 1 MB, so an oversized conversation would fail down in the
 * ZooKeeper layer with an error the user cannot act on. Rejecting it here with a 413
 * keeps the failure legible.
 */
public class TestTabConversationStore extends ClusterTest {

  private static final int TIMEOUT = 30;
  private static final MediaType JSON = MediaType.parse("application/json");
  private static int portNumber;

  private static final OkHttpClient httpClient = new OkHttpClient.Builder()
      .connectTimeout(TIMEOUT, TimeUnit.SECONDS)
      .writeTimeout(TIMEOUT, TimeUnit.SECONDS)
      .readTimeout(TIMEOUT, TimeUnit.SECONDS)
      .build();

  private static final ObjectMapper mapper = JacksonUtils.createObjectMapper();

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher)
        .configProperty(ExecConstants.HTTP_ENABLE, true)
        .configProperty(ExecConstants.HTTP_PORT_HUNT, true);
    startCluster(builder);
    portNumber = cluster.drillbit().getWebServerPort();
  }

  private static String url(String path) {
    return String.format("http://localhost:%d%s", portNumber, path);
  }

  private static JsonNode get(String path) throws Exception {
    Request request = new Request.Builder().url(url(path)).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertTrue("Expected 2xx from GET " + path + " but got " + response.code(),
          response.code() >= 200 && response.code() < 300);
      return mapper.readTree(response.body().string());
    }
  }

  private static int getStatus(String path) throws Exception {
    Request request = new Request.Builder().url(url(path)).build();
    try (Response response = httpClient.newCall(request).execute()) {
      return response.code();
    }
  }

  private static JsonNode post(String path, String jsonBody) throws Exception {
    RequestBody body = RequestBody.create(jsonBody, JSON);
    Request request = new Request.Builder().url(url(path)).post(body).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertTrue("Expected 2xx from POST " + path + " but got " + response.code(),
          response.code() >= 200 && response.code() < 300);
      return mapper.readTree(response.body().string());
    }
  }

  private static int put(String path, String jsonBody) throws Exception {
    RequestBody body = RequestBody.create(jsonBody, JSON);
    Request request = new Request.Builder().url(url(path)).put(body).build();
    try (Response response = httpClient.newCall(request).execute()) {
      return response.code();
    }
  }

  private static int delete(String path) throws Exception {
    Request request = new Request.Builder().url(url(path)).delete().build();
    try (Response response = httpClient.newCall(request).execute()) {
      return response.code();
    }
  }

  private static String newTab(String project) throws Exception {
    return post("/api/v1/tabs",
        "{\"name\":\"T\",\"sql\":\"SELECT 1\",\"projectId\":\"" + project + "\"}")
        .get("id").asText();
  }

  @Test
  public void testConversationRoundTrips() throws Exception {
    String id = newTab("conv");

    assertEquals(200, put("/api/v1/tabs/" + id + "/conversation",
        "{\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}"));

    JsonNode messages = get("/api/v1/tabs/" + id + "/conversation").get("messages");
    assertEquals(1, messages.size());
    assertEquals("hi", messages.get(0).get("content").asText());
  }

  /** A tab that has never been talked to reports an empty conversation, not a 404. */
  @Test
  public void testUnwrittenConversationIsEmpty() throws Exception {
    String id = newTab("convempty");
    assertEquals(0, get("/api/v1/tabs/" + id + "/conversation").get("messages").size());
  }

  @Test
  public void testConversationIsReplacedNotAppended() throws Exception {
    String id = newTab("convreplace");

    put("/api/v1/tabs/" + id + "/conversation",
        "{\"messages\":[{\"role\":\"user\",\"content\":\"first\"}]}");
    put("/api/v1/tabs/" + id + "/conversation",
        "{\"messages\":[{\"role\":\"user\",\"content\":\"second\"}]}");

    JsonNode messages = get("/api/v1/tabs/" + id + "/conversation").get("messages");
    assertEquals(1, messages.size());
    assertEquals("second", messages.get(0).get("content").asText());
  }

  @Test
  public void testDeletingATabDeletesItsConversation() throws Exception {
    String id = newTab("convdel");
    put("/api/v1/tabs/" + id + "/conversation",
        "{\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}");

    assertEquals(200, delete("/api/v1/tabs/" + id));

    assertEquals(404, getStatus("/api/v1/tabs/" + id + "/conversation"));
  }

  /** Two tabs must not share a thread; that is the whole point of per-tab. */
  @Test
  public void testConversationsAreSeparatePerTab() throws Exception {
    String first = newTab("convsep");
    String second = newTab("convsep");

    put("/api/v1/tabs/" + first + "/conversation",
        "{\"messages\":[{\"role\":\"user\",\"content\":\"about first\"}]}");

    assertEquals(0, get("/api/v1/tabs/" + second + "/conversation").get("messages").size());
    assertEquals("about first",
        get("/api/v1/tabs/" + first + "/conversation").get("messages").get(0)
            .get("content").asText());
  }

  /**
   * Guards ZooKeeper's 1 MB znode default. Rejected loudly rather than truncated
   * silently: a conversation quietly losing its earliest messages is worse than a
   * refused write the caller can report.
   */
  @Test
  public void testOversizedConversationIsRejected() throws Exception {
    String id = newTab("convbig");

    StringBuilder huge = new StringBuilder("{\"messages\":[{\"role\":\"user\",\"content\":\"");
    for (int i = 0; i < 600000; i++) {
      huge.append('x');
    }
    huge.append("\"}]}");

    assertEquals(413, put("/api/v1/tabs/" + id + "/conversation", huge.toString()));
  }

  /** A rejected oversized write must not clobber what was already stored. */
  @Test
  public void testRejectedWriteLeavesThePreviousConversationIntact() throws Exception {
    String id = newTab("convkeep");
    put("/api/v1/tabs/" + id + "/conversation",
        "{\"messages\":[{\"role\":\"user\",\"content\":\"keep me\"}]}");

    StringBuilder huge = new StringBuilder("{\"messages\":[{\"role\":\"user\",\"content\":\"");
    for (int i = 0; i < 600000; i++) {
      huge.append('y');
    }
    huge.append("\"}]}");
    assertEquals(413, put("/api/v1/tabs/" + id + "/conversation", huge.toString()));

    assertEquals("keep me",
        get("/api/v1/tabs/" + id + "/conversation").get("messages").get(0)
            .get("content").asText());
  }

  @Test
  public void testConversationForAnUnknownTabIsNotFound() throws Exception {
    assertEquals(404, getStatus("/api/v1/tabs/no-such-tab/conversation"));
  }
}
