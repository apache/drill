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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.rest.ProjectResources.Project;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The upload path with {@code drill.exec.impersonation} on, which routes the write
 * through the uploading user's UGI instead of the Drillbit process user. The write
 * itself is the point: a broken {@code doAs} wrapper (a missing proxy UGI, an
 * unwrapped checked exception) fails the upload rather than silently writing as the
 * wrong user, so this covers the wiring that cannot be checked after the fact.
 *
 * <p>What it cannot cover here is the resulting file ownership: the local file system
 * writes as the OS process regardless of the Hadoop UGI, so only a secured cluster
 * with a real distributed file system would show the ownership difference.
 */
public class TestProjectFileResourcesImpersonation extends ClusterTest {

  private static final MediaType JSON = MediaType.parse("application/json");
  private static final MediaType OCTET = MediaType.parse("application/octet-stream");
  private static int portNumber;
  private static final OkHttpClient client = new OkHttpClient.Builder()
      .connectTimeout(30, TimeUnit.SECONDS)
      .readTimeout(30, TimeUnit.SECONDS)
      .build();

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(new ClusterFixtureBuilder(dirTestWatcher)
        .configProperty(ExecConstants.HTTP_ENABLE, true)
        .configProperty(ExecConstants.HTTP_PORT_HUNT, true)
        .configProperty(ExecConstants.ALLOW_LOOPBACK_ADDRESS_BINDING, true)
        .configProperty(ExecConstants.IMPERSONATION_ENABLED, true));
    portNumber = cluster.drillbit().getWebServerPort();
  }

  @Test
  public void testUploadWritesThroughTheUserUgi() throws Exception {
    assertTrue(cluster.drillbit().getContext().getConfig()
        .getBoolean(ExecConstants.IMPERSONATION_ENABLED));

    Request configure = new Request.Builder()
        .url("http://localhost:" + portNumber + "/api/v1/uploads/config")
        .put(RequestBody.create(
            "{\"plugin\":\"dfs\",\"workspace\":\"tmp\",\"maxSizeBytes\":1000}", JSON))
        .build();
    try (Response r = client.newCall(configure).execute()) {
      assertEquals(r.body().string(), 200, r.code());
    }

    String id = UUID.randomUUID().toString();
    ProjectResources.openStore(cluster.drillbit().getContext().getStoreProvider(),
        cluster.drillbit().getManager())
        .put(id, new Project(id, "Uploads", null, null, DrillUserPrincipal.ANONYMOUS_USER,
            false, Collections.<String>emptyList(), Collections.<String>emptyList(),
            null, null, null, null, null, false, 1L, 1L, null, null, 0L));

    MultipartBody body = new MultipartBody.Builder()
        .setType(MultipartBody.FORM)
        .addFormDataPart("file", "sales.csv",
            RequestBody.create("x,y\n1,2\n".getBytes(StandardCharsets.UTF_8), OCTET))
        .build();
    Request upload = new Request.Builder()
        .url("http://localhost:" + portNumber + "/api/v1/projects/" + id + "/files")
        .post(body)
        .build();
    try (Response r = client.newCall(upload).execute()) {
      assertEquals(r.body().string(), 201, r.code());
    }

    File written = new File(new File(dirTestWatcher.getDfsTestTmpDir(), id), "sales.csv");
    assertTrue(written + " was not written", written.exists());
    assertEquals(8, written.length());
  }
}
