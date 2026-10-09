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

package org.apache.drill.exec.store.sentinel;

import okhttp3.mockwebserver.MockWebServer;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class SentinelTestBase extends ClusterTest {
  protected static MockWebServer mockWebServer;

  @BeforeClass
  public static void setUpCluster() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));

    mockWebServer = new MockWebServer();
    mockWebServer.start();
  }

  protected static String loadFixture(String filename) throws IOException {
    String path = "src/test/resources/responses/" + filename;
    return new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
  }

  protected static String getMockServerUrl() {
    return mockWebServer.url("/").toString();
  }
}
