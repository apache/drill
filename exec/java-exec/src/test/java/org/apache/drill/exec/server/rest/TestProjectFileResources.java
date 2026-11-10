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
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.rest.ProjectResources.DatasetRef;
import org.apache.drill.exec.server.rest.ProjectResources.Project;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
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

    // "." and "foo:bar" pass safeName and are stopped only by the normalize-and-prefix
    // check: Hadoop resolves the first back onto the folder itself and the second, which
    // it reads as a scheme-bearing URI, outside the folder entirely.
    for (String name : Arrays.asList("../../etc/passwd", "a/b.csv", "..\\win.csv", "..",
        ".", "foo:bar")) {
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
    assertNull(ProjectFileResources.safeName("a\nFAKE LOG LINE.csv"));
    // Long enough that the staging suffix would push the path component past 255 bytes.
    assertNull(ProjectFileResources.safeName(new String(new char[212]).replace('\0', 'a')));
    assertEquals(211, ProjectFileResources.safeName(
        new String(new char[211]).replace('\0', 'a')).length());
    assertNull(ProjectFileResources.safeName("a\rb.csv"));
    assertEquals("sales.csv", ProjectFileResources.safeName(" sales.csv "));
    // Format-agnostic: an extension Drill cannot read is still a legal upload name.
    assertEquals("notes.zzz", ProjectFileResources.safeName("notes.zzz"));
  }

  /**
   * A declared length past the cap is refused from the request header, before Jersey
   * spools the part to the Drillbit's temp directory to inject it.
   */
  @Test
  public void testRejectsOversizeContentLengthBeforeReadingTheEntity() throws Exception {
    configureUploads(10);
    String id = seedWritableProject();
    byte[] tooBig = new byte[(int) ProjectFileResources.allowedRequestBytes(10) + 1];
    try (Response r = post(id, "huge.csv", tooBig, false)) {
      assertEquals(413, r.code());
    }
    // The discriminator: the handler creates the project folder before it starts copying,
    // so an absent folder is proof the request never reached the handler at all.
    assertFalse(projectFolder(id).exists());
  }

  @Test
  public void testRequestAllowanceCoversTheMultipartEnvelopeAndSaturates() {
    assertTrue(ProjectFileResources.allowedRequestBytes(10) > 10);
    assertEquals(Long.MAX_VALUE, ProjectFileResources.allowedRequestBytes(Long.MAX_VALUE));
  }

  /**
   * An aborted upload must not cost the file already in place. The staged-then-renamed
   * write is what makes this pass; truncating the target up front would not.
   */
  @Test
  public void testOversizeOverwriteLeavesTheExistingFileIntact() throws Exception {
    configureUploads(1000);
    String id = seedWritableProject();
    upload(id, "keep.csv", "original", false, 201);

    configureUploads(5);
    upload(id, "keep.csv", "12345678901", true, 413);

    assertEquals("original", contents(new File(projectFolder(id), "keep.csv")));
    assertEquals(0, stagedFiles(id).length);
  }

  /** No {@code .upload-} leftovers after a successful write either. */
  @Test
  public void testSuccessfulUploadLeavesNoStagingFile() throws Exception {
    configureUploads(1000);
    String id = seedWritableProject();
    upload(id, "clean.csv", "a\n1\n", false, 201);
    assertEquals(0, stagedFiles(id).length);
  }

  /**
   * A workspace location given without a leading slash. Pinned because the composition
   * resolves it against the connection root rather than any process directory.
   */
  @Test
  public void testProjectDirResolvesARelativeWorkspaceLocation() {
    UploadConfigResources.UploadConfig cfg =
        new UploadConfigResources.UploadConfig("dfs", "rel", 100L);
    FileSystemConfig fsConfig = new FileSystemConfig("file:///",
        Collections.<String, String>emptyMap(),
        Collections.singletonMap("rel", new WorkspaceConfig("uploads", true, null, false)),
        Collections.<String, FormatPluginConfig>emptyMap(), null);
    assertEquals("file:/uploads/p1",
        ProjectFileResources.projectDir(cfg, fsConfig, "p1").toUri().toString());

    FileSystemConfig s3 = new FileSystemConfig("s3a://bucket",
        Collections.<String, String>emptyMap(),
        Collections.singletonMap("rel", new WorkspaceConfig("/uploads", true, null, false)),
        Collections.<String, FormatPluginConfig>emptyMap(), null);
    assertEquals("s3a://bucket/uploads/p1",
        ProjectFileResources.projectDir(cfg, s3, "p1").toUri().toString());
  }

  /**
   * The window the staging exists for: the move onto the target fails, so neither the
   * bytes already there nor the ones just uploaded may be thrown away. Forced by putting
   * a non-empty directory where the file would go, which no rename can land on and no
   * non-recursive delete can clear. No production code is bent to make this happen.
   */
  @Test
  public void testFailedMoveKeepsThePriorContentAndTheUploadedBytes() throws Exception {
    configureUploads(1000);
    String id = seedWritableProject();
    File blocked = new File(projectFolder(id), "blocked.csv");
    File prior = new File(blocked, "part-0");
    assertTrue(blocked.mkdirs());
    Files.write(prior.toPath(), "prior".getBytes(StandardCharsets.UTF_8));

    upload(id, "blocked.csv", "new bytes", true, 500);

    assertEquals("prior", contents(prior));
    File[] staged = stagedFiles(id);
    assertEquals(1, staged.length);
    assertEquals("new bytes", contents(staged[0]));
  }

  @Test
  public void testRejectsOverlongName() throws Exception {
    configureUploads(1000);
    String id = seedWritableProject();
    String tooLong = new String(new char[212]).replace('\0', 'a') + ".csv";
    JsonNode body = upload(id, tooLong, "x\n1\n", false, 400);
    assertTrue(body.get("message").asText(), body.get("message").asText().contains("name"));
    assertFalse(projectFolder(id).exists());
  }

  /**
   * Staged uploads left in a project folder. Hidden files are skipped: the local file
   * system keeps a {@code .<name>.crc} sidecar beside each one, which Drill ignores when
   * it reads the folder.
   */
  private static File[] stagedFiles(String projectId) {
    File[] staged = projectFolder(projectId)
        .listFiles((d, n) -> n.contains(".upload-") && !n.startsWith("."));
    return staged == null ? new File[0] : staged;
  }

  private static String contents(File file) throws Exception {
    return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
  }

  // ==================== Delete and download ====================

  private static Response send(String method, String projectId, String nameSegment,
      String suffix) throws Exception {
    Request.Builder b = new Request.Builder().url("http://localhost:" + portNumber
        + "/api/v1/projects/" + projectId + "/files/" + nameSegment + suffix);
    if ("DELETE".equals(method)) {
      b.delete();
    } else if ("HEAD".equals(method)) {
      b.head();
    } else {
      b.get();
    }
    return client.newCall(b.build()).execute();
  }

  @Test
  public void testDeleteRemovesFile() throws Exception {
    configureUploads(1000);
    String id = seedWritableProject();
    upload(id, "gone.csv", "a\n1\n", false, 201);
    File file = new File(projectFolder(id), "gone.csv");
    assertTrue(file.exists());

    try (Response r = send("DELETE", id, "gone.csv", "")) {
      assertEquals(204, r.code());
    }
    assertFalse(file.exists());

    try (Response r = send("DELETE", id, "gone.csv", "")) {
      assertEquals(404, r.code());
    }
  }

  @Test
  public void testDeleteRejectsPathTraversal() throws Exception {
    configureUploads(1000);
    String id = seedWritableProject();
    upload(id, "keep.csv", "a\n1\n", false, 201);
    File outside = new File(dirTestWatcher.getDfsTestTmpDir(), "outside.csv");
    Files.write(outside.toPath(), "x".getBytes(StandardCharsets.UTF_8));
    try {
      // The backslash and parent-reference forms fail safeName; "foo:bar" passes it and
      // is stopped only by the normalize-and-prefix check.
      for (String name : Arrays.asList("..%5Coutside.csv", "a%5Cb.csv", "foo:bar")) {
        try (Response r = send("DELETE", id, name, "")) {
          assertEquals(name, 400, r.code());
        }
      }
      assertTrue(outside.exists());
      assertTrue(new File(projectFolder(id), "keep.csv").exists());
    } finally {
      outside.delete();
    }
  }

  @Test
  public void testDeleteRejectsWithoutWriteAccess() throws Exception {
    configureUploads(1000);
    String id = seedProject("someone-else", Collections.<String>emptyList(),
        Arrays.asList(DrillUserPrincipal.ANONYMOUS_USER));
    File file = new File(projectFolder(id), "mine.csv");
    assertTrue(file.getParentFile().mkdirs());
    Files.write(file.toPath(), "x".getBytes(StandardCharsets.UTF_8));

    try (Response r = send("DELETE", id, "mine.csv", "")) {
      assertEquals(403, r.code());
    }
    assertTrue(file.exists());
  }

  @Test
  public void testDownloadStreamsBytes() throws Exception {
    configureUploads(1000);
    String id = seedWritableProject();
    byte[] bytes = new byte[] {0, 1, 2, (byte) 0xff, 10, 13, 65};
    try (Response up = post(id, "blob.bin", bytes, false)) {
      assertEquals(201, up.code());
    }
    try (Response r = send("GET", id, "blob.bin", "/content")) {
      assertEquals(200, r.code());
      assertTrue(r.header("Content-Type"), r.header("Content-Type").startsWith(
          "application/octet-stream"));
      assertTrue(r.header("Content-Disposition"),
          r.header("Content-Disposition").contains("filename=\"blob.bin\""));
      assertTrue(Arrays.equals(bytes, r.body().bytes()));
    }
  }

  @Test
  public void testDownloadAllowsViewer() throws Exception {
    configureUploads(1000);
    String id = seedProject("someone-else", Collections.<String>emptyList(),
        Arrays.asList(DrillUserPrincipal.ANONYMOUS_USER));
    File file = new File(projectFolder(id), "shared.csv");
    assertTrue(file.getParentFile().mkdirs());
    Files.write(file.toPath(), "viewable".getBytes(StandardCharsets.UTF_8));

    try (Response r = send("GET", id, "shared.csv", "/content")) {
      assertEquals(200, r.code());
      assertEquals("viewable", r.body().string());
    }
  }

  @Test
  public void testDownloadMissingFileIsNotFound() throws Exception {
    configureUploads(1000);
    String id = seedWritableProject();
    // Same answer whether or not the project folder exists.
    try (Response r = send("GET", id, "nope.csv", "/content")) {
      assertEquals(404, r.code());
    }
    upload(id, "there.csv", "a\n", false, 201);
    try (Response r = send("GET", id, "nope.csv", "/content")) {
      assertEquals(404, r.code());
    }
  }

  @Test
  public void testDownloadRejectsPathTraversal() throws Exception {
    configureUploads(1000);
    String id = seedWritableProject();
    for (String name : Arrays.asList("..%5Cx.csv", "foo:bar")) {
      try (Response r = send("GET", id, name, "/content")) {
        assertEquals(name, 400, r.code());
      }
    }
  }

  /** HEAD is auto-served from the GET handler with the body dropped; it must not leak. */
  @Test
  public void testHeadOnDownloadSucceedsWithoutBody() throws Exception {
    configureUploads(1000);
    String id = seedWritableProject();
    upload(id, "h.csv", "a\n1\n", false, 201);
    try (Response r = send("HEAD", id, "h.csv", "/content")) {
      assertEquals(200, r.code());
      assertEquals(0, r.body().bytes().length);
    }
    try (Response r = send("HEAD", id, "nope.csv", "/content")) {
      assertEquals(404, r.code());
    }
  }

  @Test
  public void testDeleteOfADirectoryIsNotFoundAndLeavesItIntact() throws Exception {
    configureUploads(1000);
    String id = seedWritableProject();
    File dir = new File(projectFolder(id), "sub");
    assertTrue(dir.mkdirs());
    Files.write(new File(dir, "inner.csv").toPath(), "x".getBytes(StandardCharsets.UTF_8));
    try (Response r = send("DELETE", id, "sub", "")) {
      assertEquals(404, r.code());
    }
    assertTrue(new File(dir, "inner.csv").exists());
  }

  /**
   * Called directly: Jetty and OkHttp normalize "." away before any handler sees it, so
   * it cannot be sent over HTTP. This is the check upload, delete and download share.
   */
  @Test
  public void testResolveInsideRefusesNamesThatEscapeTheFolder() {
    org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(new org.apache.hadoop.fs.Path("file:///"), "data/p1");
    for (String name : Arrays.asList(".", "foo:bar")) {
      try {
        ProjectFileResources.resolveInside(dir, name);
        org.junit.Assert.fail("expected a refusal for " + name);
      } catch (IllegalArgumentException expected) {
        assertTrue(expected.getMessage(), expected.getMessage().contains("name"));
      }
    }
    assertEquals("file:/data/p1/a.csv",
        ProjectFileResources.resolveInside(dir, "a.csv").toUri().toString());
  }
}
