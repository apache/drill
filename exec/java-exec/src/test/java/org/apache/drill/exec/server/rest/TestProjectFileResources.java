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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.rest.ProjectResources.DatasetRef;
import org.apache.drill.exec.server.rest.ProjectResources.Project;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The project file upload endpoint. Uploads land in the cluster's configured upload
 * target, here the stock writable {@code dfs.tmp} workspace, so each assertion about
 * "nothing written" can be made against the real directory on disk.
 */
public class TestProjectFileResources extends ClusterTest {

  private static final MediaType JSON = MediaType.parse("application/json");
  private static final MediaType OCTET = MediaType.parse("application/octet-stream");
  private static final ObjectMapper MAPPER = new ObjectMapper();
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

  // ==================== Fixtures ====================

  /** Points the cluster's upload target at {@code dfs.tmp} with the given size cap. */
  private static void configureUploads(long maxSizeBytes) throws Exception {
    Request request = new Request.Builder()
        .url("http://localhost:" + portNumber + "/api/v1/uploads/config")
        .put(RequestBody.create("{\"plugin\":\"dfs\",\"workspace\":\"tmp\",\"maxSizeBytes\":"
            + maxSizeBytes + "}", JSON))
        .build();
    try (Response r = client.newCall(request).execute()) {
      assertEquals(r.body().string(), 200, r.code());
    }
  }

  private static void clearUploadConfig() {
    UploadConfigResources.clearForTest(cluster.drillbit().getContext().getStoreProvider());
  }

  private static PersistentStore<Project> projectStore() {
    return ProjectResources.openStore(cluster.drillbit().getContext().getStoreProvider(),
        cluster.drillbit().getManager());
  }

  /** Seeds a project the requesting (anonymous) user may write to. */
  private static String seedWritableProject() {
    return seedProject(DrillUserPrincipal.ANONYMOUS_USER,
        Collections.<String>emptyList(), Collections.<String>emptyList());
  }

  private static String seedProject(String owner, List<String> sharedWith, List<String> viewers) {
    String id = UUID.randomUUID().toString();
    projectStore().put(id, new Project(id, "Uploads", null, null, owner, false,
        sharedWith, viewers, null, null, null, null, null, false, 1L, 1L, null, null, 0L));
    return id;
  }

  private static File projectFolder(String projectId) {
    return new File(dirTestWatcher.getDfsTestTmpDir(), projectId);
  }

  private static Response post(String projectId, String fileName, byte[] content,
      boolean overwrite) throws Exception {
    MultipartBody body = new MultipartBody.Builder()
        .setType(MultipartBody.FORM)
        .addFormDataPart("file", fileName, RequestBody.create(content, OCTET))
        .build();
    Request request = new Request.Builder()
        .url("http://localhost:" + portNumber + "/api/v1/projects/" + projectId + "/files"
            + (overwrite ? "?overwrite=true" : ""))
        .post(body)
        .build();
    return client.newCall(request).execute();
  }

  /** Posts a part and asserts the status code, returning the parsed body. */
  private static JsonNode upload(String projectId, String fileName, String content,
      boolean overwrite, int expected) throws Exception {
    try (Response r = post(projectId, fileName, content.getBytes(StandardCharsets.UTF_8),
        overwrite)) {
      String responseBody = r.body().string();
      assertEquals(responseBody, expected, r.code());
      return MAPPER.readTree(responseBody);
    }
  }

  private static List<DatasetRef> folderRefs(String projectId) {
    List<DatasetRef> all = projectStore().get(projectId).getDatasets();
    return all.stream().filter(d -> "folder".equals(d.getType()))
        .collect(Collectors.toList());
  }

  // ==================== Tests ====================

  @Test
  public void testRejectsPathTraversal() throws Exception {
    configureUploads(1000);
    String id = seedWritableProject();

    for (String name : Arrays.asList("../../etc/passwd", "a/b.csv", "..\\win.csv", "..")) {
      JsonNode body = upload(id, name, "x,y\n1,2\n", false, 400);
      assertTrue(body.get("message").asText(), body.get("message").asText().contains("name"));
    }

    assertFalse(projectFolder(id).exists());
    assertFalse(new File(dirTestWatcher.getDfsTestTmpDir(), "b.csv").exists());
    assertTrue(folderRefs(id).isEmpty());
  }

  @Test
  public void testRejectsWhenUnconfigured() throws Exception {
    clearUploadConfig();
    String id = seedWritableProject();
    try {
      JsonNode body = upload(id, "a.csv", "x\n1\n", false, 409);
      assertTrue(body.get("message").asText(),
          body.get("message").asText().contains("upload folder"));
    } finally {
      configureUploads(1000);
    }
  }

  @Test
  public void testRejectsWithoutWriteAccess() throws Exception {
    configureUploads(1000);
    // A viewer may read the project but not write to it.
    String id = seedProject("someone-else", Collections.<String>emptyList(),
        Arrays.asList(DrillUserPrincipal.ANONYMOUS_USER));
    upload(id, "a.csv", "x\n1\n", false, 403);
    assertFalse(projectFolder(id).exists());
  }

  @Test
  public void testRejectsOversizeFile() throws Exception {
    configureUploads(10);
    String id = seedWritableProject();
    JsonNode body = upload(id, "big.csv", "12345678901", false, 413);
    assertTrue(body.get("message").asText(), body.get("message").asText().contains("10"));
    assertFalse(new File(projectFolder(id), "big.csv").exists());
    assertTrue(folderRefs(id).isEmpty());
  }

  @Test
  public void testAcceptsFileAtExactlyTheSizeLimit() throws Exception {
    configureUploads(10);
    String id = seedWritableProject();
    upload(id, "exact.csv", "1234567890", false, 201);
    assertEquals(10, new File(projectFolder(id), "exact.csv").length());
  }

  @Test
  public void testCollisionRequiresOverwrite() throws Exception {
    configureUploads(1000);
    String id = seedWritableProject();
    File target = new File(projectFolder(id), "dup.csv");

    upload(id, "dup.csv", "first", false, 201);
    assertEquals("first", contents(target));

    JsonNode conflict = upload(id, "dup.csv", "second", false, 409);
    assertTrue(conflict.get("message").asText(),
        conflict.get("message").asText().contains("overwrite"));
    assertEquals("first", contents(target));

    upload(id, "dup.csv", "second", true, 201);
    assertEquals("second", contents(target));
  }

  @Test
  public void testUploadWritesFileAndRegistersFolderDataset() throws Exception {
    configureUploads(1000);
    String id = seedWritableProject();

    JsonNode body = upload(id, "sales.csv", "x,y\n1,2\n", false, 201);
    assertEquals("sales.csv", body.get("name").asText());
    assertEquals("sales.csv", body.get("path").asText());
    assertEquals("dfs.tmp." + id, body.get("schema").asText());
    assertEquals("x,y\n1,2\n", contents(new File(projectFolder(id), "sales.csv")));

    List<DatasetRef> refs = folderRefs(id);
    assertEquals(1, refs.size());
    assertEquals("dfs.tmp", refs.get(0).getSchema());
    assertEquals(id, refs.get(0).getTable());
    assertEquals("Uploaded files", refs.get(0).getLabel());

    // A second upload reuses the one folder reference.
    upload(id, "costs.csv", "a\n1\n", false, 201);
    assertEquals(1, folderRefs(id).size());
    assertTrue(new File(projectFolder(id), "costs.csv").exists());
  }

  @Test
  public void testRejectsUnknownProject() throws Exception {
    configureUploads(1000);
    upload("no-such-project", "a.csv", "x\n1\n", false, 404);
  }

  @Test
  public void testRejectsMissingPart() throws Exception {
    configureUploads(1000);
    String id = seedWritableProject();
    MultipartBody body = new MultipartBody.Builder()
        .setType(MultipartBody.FORM)
        .addFormDataPart("notthefile", "a.csv", RequestBody.create("x", OCTET))
        .build();
    Request request = new Request.Builder()
        .url("http://localhost:" + portNumber + "/api/v1/projects/" + id + "/files")
        .post(body)
        .build();
    try (Response r = client.newCall(request).execute()) {
      assertEquals(400, r.code());
    }
  }

  @Test
  public void testSafeNameRefusesSeparatorsAndParentReferences() {
    assertNull(ProjectFileResources.safeName("a/b.csv"));
    assertNull(ProjectFileResources.safeName("a\\b.csv"));
    assertNull(ProjectFileResources.safeName("../../etc/passwd"));
    assertNull(ProjectFileResources.safeName(".."));
    assertNull(ProjectFileResources.safeName("  "));
    assertNull(ProjectFileResources.safeName(null));
    assertEquals("sales.csv", ProjectFileResources.safeName(" sales.csv "));
    // Format-agnostic: an extension Drill cannot read is still a legal upload name.
    assertEquals("notes.zzz", ProjectFileResources.safeName("notes.zzz"));
  }

  private static String contents(File file) throws Exception {
    return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
  }
}
