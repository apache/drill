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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.drill.common.util.JacksonUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.work.WorkManager;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for the project schema-cache REST endpoints
 * ({@code POST /{id}/schema-cache/refresh} and {@code GET /{id}/schema-cache}).
 * These exercise the scan-on-refresh path wired into {@link ProjectResources}
 * and verify the cached schema metadata is persisted and returned.
 */
public class TestProjectSchemaCacheEndpoints extends ClusterTest {

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

  private String url(String path) {
    return String.format("http://localhost:%d%s", portNumber, path);
  }

  private JsonNode post(String path, String jsonBody) throws Exception {
    RequestBody body = RequestBody.create(jsonBody == null ? "" : jsonBody, JSON);
    Request request = new Request.Builder().url(url(path)).post(body).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertTrue(response.code() >= 200 && response.code() < 300,
          "Expected 2xx from POST " + path + " but got " + response.code());
      return mapper.readTree(response.body().string());
    }
  }

  @Test
  public void testRefreshAndGetSchemaCache() throws Exception {
    // 1. Create a project.
    JsonNode project = post("/api/v1/projects",
        "{\"name\":\"Schema Cache Test\"}");
    String id = project.get("id").asText();
    assertTrue(id != null && !id.isEmpty(), "Created project should have an id");

    // 2. Add a dataset referencing a real, scannable schema (dfs.tmp).
    post("/api/v1/projects/" + id + "/datasets",
        "{\"type\":\"schema\",\"schema\":\"dfs.tmp\",\"label\":\"tmp\"}");

    // 3. Refresh the schema cache; this triggers a live INFORMATION_SCHEMA scan.
    JsonNode refreshed = post("/api/v1/projects/" + id + "/schema-cache/refresh", null);
    assertTrue(refreshed.has("schemas"), "Refresh response should contain a schemas object");
    assertTrue(refreshed.get("schemas").has("dfs.tmp"),
        "Refresh response should have scanned the dfs.tmp schema");

    // 4. Read the cache back and assert the scan was persisted.
    Request getReq = new Request.Builder()
        .url(url("/api/v1/projects/" + id + "/schema-cache")).build();
    try (Response response = httpClient.newCall(getReq).execute()) {
      assertEquals(200, response.code());
      JsonNode json = mapper.readTree(response.body().string());
      assertTrue(json.has("schemas"), "Cache entry should contain a schemas object");
      JsonNode schemas = json.get("schemas");
      assertTrue(schemas.has("dfs.tmp"),
          "Cached schemas should contain the added dfs.tmp key");
      JsonNode dfsTmp = schemas.get("dfs.tmp");
      assertTrue(dfsTmp.get("scannedAt").asLong() > 0,
          "Cached schema dfs.tmp should have a scannedAt timestamp > 0");
    }
  }

  /**
   * Seeds a project owned by another user (not the anonymous admin the harness
   * runs as) directly into the projects store, then seeds its schema cache with a
   * distinctively-named table the live INFORMATION_SCHEMA scan could never produce.
   *
   * <p>Because the cluster harness issues every HTTP request as the same anonymous
   * admin user, an owner of {@code "someone-else"} with {@code isPublic = false}
   * makes {@link ProjectResources#canRead} return {@code false} for the requester —
   * exactly the condition the access-control gate must enforce.
   *
   * @return the id of the seeded foreign project
   */
  private static String seedForeignProject(String secretTable) {
    String id = UUID.randomUUID().toString();
    ProjectResources.DatasetRef dataset =
        new ProjectResources.DatasetRef("ds-1", "schema", "dfs.tmp", null, null, "tmp");
    ProjectResources.Project foreign = new ProjectResources.Project(
        id,                                        // id
        "Foreign Project",                         // name
        null,                                      // description
        null,                                      // tags
        "someone-else",                            // owner (NOT the requester)
        false,                                     // isPublic
        null,                                      // sharedWith
        Collections.singletonList(dataset),        // datasets
        null,                                      // savedQueryIds
        null,                                      // visualizationIds
        null,                                      // dashboardIds
        null,                                      // wikiPages
        false,                                     // isSystem
        System.currentTimeMillis(),                // createdAt
        System.currentTimeMillis(),                // updatedAt
        null,                                      // tileColor
        null,                                      // tileImage
        0L);                                       // deletedAt (active)

    PersistentStoreProvider provider = cluster.drillbit().getContext().getStoreProvider();
    WorkManager workManager = cluster.drillbit().getManager();

    // Write the project directly; there is no HTTP path to create a project owned by
    // another user, since the harness authenticates as anonymous.
    ProjectResources.openStore(provider, workManager).put(id, foreign);

    // Seed the cache with a fake table the live query can never return, so any leak
    // of cached data through the gate would be unambiguous.
    ProjectSchemaCache.get(provider, workManager).scan(id, "dfs.tmp", schemaPath -> {
      ProjectSchemaCache.CachedTable table = new ProjectSchemaCache.CachedTable();
      table.setSchema("dfs.tmp");
      table.setName(secretTable);
      table.setType("TABLE");
      table.setColumns(new ArrayList<>());
      return Collections.singletonList(table);
    });
    return id;
  }

  /**
   * A user who cannot read a project must not be served its schema cache via
   * {@code GET /{id}/schema-cache}. Owner mismatch (the project is owned by
   * "someone-else" and is not public) must yield 403.
   *
   * <p>Non-vacuous: temporarily removing the {@code canRead} check in
   * {@link ProjectResources#getSchemaCache} makes this return 200 with the seeded
   * entry, so the assertion below fails — confirming the gate is what produces the
   * deny.
   */
  @Test
  public void testSchemaCacheDeniedForUnreadableProject() throws Exception {
    String id = seedForeignProject("secret_table");

    Request getReq = new Request.Builder()
        .url(url("/api/v1/projects/" + id + "/schema-cache")).build();
    try (Response response = httpClient.newCall(getReq).execute()) {
      assertEquals(403, response.code(),
          "A project the caller cannot read must return 403 from GET /schema-cache");
    }
  }

  /**
   * The metadata tables endpoint must not serve a project's cache to a user who
   * cannot read that project. With the gate active, {@code cachedSchemaFor} returns
   * null for the unreadable project and the endpoint falls through to the live
   * INFORMATION_SCHEMA query, which cannot contain the fake {@code secret_table}
   * seeded only into the cache.
   *
   * <p>Non-vacuous: the seeded {@code secret_table} exists only in the cache and can
   * never come back from the live scan of the empty {@code dfs.tmp} directory. If the
   * {@code canRead} gate in {@code cachedSchemaFor} were bypassed, the cached entry
   * (fresh, so not rescanned) would be served and {@code secret_table} WOULD appear —
   * making the absence assertion below fail.
   */
  @Test
  public void testMetadataTablesDoesNotLeakCacheForUnreadableProject() throws Exception {
    String secretTable = "secret_table_" + UUID.randomUUID().toString().replace("-", "");
    String id = seedForeignProject(secretTable);

    Request getReq = new Request.Builder()
        .url(url("/api/v1/metadata/schemas/dfs.tmp/tables?projectId=" + id)).build();
    try (Response response = httpClient.newCall(getReq).execute()) {
      assertEquals(200, response.code());
      JsonNode json = mapper.readTree(response.body().string());
      JsonNode tables = json.get("tables");
      boolean leaked = false;
      if (tables != null) {
        for (JsonNode t : tables) {
          if (secretTable.equals(t.path("name").asText())) {
            leaked = true;
            break;
          }
        }
      }
      assertFalse(leaked,
          "Cache-only table " + secretTable + " must not be served for a project the caller cannot read");
    }
  }
}
