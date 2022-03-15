package org.apache.drill.exec.udfs.http;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.drill.exec.util.HttpUtils;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestHttpUDFFunctions extends ClusterTest {

  private static final int MOCK_SERVER_PORT = 47770;

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void testHttpGetWithEmptyArg() throws Exception {
    try (MockWebServer server = startServer()) {
      server.enqueue(new MockResponse().setResponseCode(200).setBody(TEST_JSON_RESPONSE_WITH_DATATYPES));
      String query = "SELECT http_get('http://localhost:7777') AS result FROM (values(1))";
      testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("result")
        .go();
    }
  }

  @Test
  public void testPositionalReplacement() {
    String url = "http://somesite.com/{p1}/{p2}/path/{}";
    List<String> params = new ArrayList<>();
    params.add("foo");
    params.add("bar");
    params.add("baz");
    assertEquals("http://somesite.com/foo/bar/path/baz", HttpUtils.mapPositionalParameters(url, params));
  }

  /**
   * Helper function to start the MockHTTPServer
   * @return Started Mock server
   * @throws IOException If the server cannot start, throws IOException
   */
  public static MockWebServer startServer() throws IOException {
    MockWebServer server = new MockWebServer();
    server.start(MOCK_SERVER_PORT);
    return server;
  }
}
