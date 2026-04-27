package org.apache.drill.exec.store.sentinel;

import okhttp3.mockwebserver.MockWebServer;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;

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
