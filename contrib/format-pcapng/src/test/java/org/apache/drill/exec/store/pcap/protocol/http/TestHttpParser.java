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
package org.apache.drill.exec.store.pcap.protocol.http;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestHttpParser extends BaseTest {

  static final class Context implements DecoderContext {
    final boolean expose;
    final List<String> warnings = new ArrayList<>();

    Context(boolean expose) {
      this.expose = expose;
    }

    @Override
    public boolean exposeCredentials() {
      return expose;
    }

    @Override
    public void warn(String message) {
      warnings.add(message);
    }
  }

  static byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.ISO_8859_1);
  }

  static HttpMessage parse(String s, boolean expose) {
    byte[] b = bytes(s);
    return HttpParser.parseMessage(b, 0, b.length, new Context(expose));
  }

  @Test
  public void testRequest() {
    HttpMessage m = parse("GET /index.html?q=1 HTTP/1.1\r\nHost: example.com\r\nUser-Agent: curl/8\r\n\r\n", false);
    assertTrue(m.isRequest);
    assertEquals("GET", m.method);
    assertEquals("/index.html?q=1", m.uri);
    assertEquals("1.1", m.version);
    assertEquals("example.com", m.header("host"));
    assertEquals(2, m.headers.size());
    assertTrue(m.headerEnd > 0);
  }

  @Test
  public void testResponse() {
    HttpMessage m = parse("HTTP/1.1 404 Not Found\r\nContent-Length: 12\r\n\r\n", false);
    assertFalse(m.isRequest);
    assertEquals(Integer.valueOf(404), m.statusCode);
    assertEquals("Not Found", m.reason);
    assertEquals(Long.valueOf(12), m.contentLength());
  }

  @Test
  public void testIncompleteHeaders() {
    HttpMessage m = parse("POST /api HTTP/1.0\r\nHost: a\r\nContent-Ty", false);
    assertEquals("POST", m.method);
    assertEquals(-1, m.headerEnd);
    assertEquals(1, m.headers.size());
  }

  @Test
  public void testNotHttp() {
    assertNull(parse("SSH-2.0-OpenSSH_9.0\r\n", false));
    assertNull(parse("get / HTTP/1.1\r\n\r\n", false));
    assertNull(parse("GET / HTTP/2\r\n\r\n", false));
    assertNull(parse("\u0016\u0003\u0001\u0002\u0000", false));
  }

  @Test
  public void testBasicAuth() {
    // dXNlcjpzM2NyZXQ= is user:s3cret
    String request = "GET / HTTP/1.1\r\nAuthorization: Basic dXNlcjpzM2NyZXQ=\r\n\r\n";
    HttpMessage hidden = parse(request, false);
    assertEquals("user", hidden.username);
    assertTrue(hidden.passwordPresent);
    assertNull(hidden.password);
    assertEquals("s3cret", parse(request, true).password);
  }

  @Test
  public void testHeaderCap() {
    StringBuilder s = new StringBuilder("GET / HTTP/1.1\r\n");
    for (int i = 0; i < 70; i++) {
      s.append("X-H").append(i).append(": v\r\n");
    }
    s.append("\r\n");
    byte[] b = bytes(s.toString());
    Context context = new Context(false);
    HttpMessage m = HttpParser.parseMessage(b, 0, b.length, context);
    assertEquals(64, m.headers.size());
    assertEquals(1, context.warnings.size());
  }

  @Test
  public void testChunked() {
    assertTrue(parse("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n", false).isChunked());
  }
}
