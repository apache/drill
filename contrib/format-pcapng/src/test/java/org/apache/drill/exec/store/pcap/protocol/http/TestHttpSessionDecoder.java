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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestHttpSessionDecoder extends BaseTest {

  @Test
  public void testKeepAliveWithChunkedBody() {
    String client = "GET /a HTTP/1.1\r\nHost: h\r\n\r\n"
        + "POST /b HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n\r\nhello"
        + "HEAD /c HTTP/1.1\r\nHost: h\r\n\r\n";
    String server = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nwiki\r\n5;ext=1\r\npedia\r\n0\r\nX-Trailer: t\r\n\r\n"
        + "HTTP/1.1 201 Created\r\nContent-Length: 2\r\n\r\nok"
        + "HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n";
    TestHttpParser.Context context = new TestHttpParser.Context(false);
    List<HttpMessage> requests = HttpSessionDecoder.parseStream(TestHttpParser.bytes(client), true,
        Collections.emptyList(), context);
    List<HttpMessage> responses = HttpSessionDecoder.parseStream(TestHttpParser.bytes(server), false, requests, context);
    assertEquals(3, requests.size());
    assertEquals("/b", requests.get(1).uri);
    assertEquals("HEAD", requests.get(2).method);
    assertEquals(3, responses.size());
    assertEquals(Integer.valueOf(201), responses.get(1).statusCode);
    // The HEAD response has Content-Length but no body
    assertEquals(Integer.valueOf(200), responses.get(2).statusCode);
    assertTrue(context.warnings.isEmpty());
  }

  @Test
  public void testNotHttpSession() {
    TestHttpParser.Context context = new TestHttpParser.Context(false);
    assertTrue(HttpSessionDecoder.parseStream(TestHttpParser.bytes("SSH-2.0-x\r\n"), true,
        Collections.emptyList(), context).isEmpty());
  }

  @Test
  public void testGarbageAfterFirstMessageWarns() {
    TestHttpParser.Context context = new TestHttpParser.Context(false);
    List<HttpMessage> requests = HttpSessionDecoder.parseStream(
        TestHttpParser.bytes("GET / HTTP/1.1\r\n\r\n\u0000\u0001garbage"), true, Collections.emptyList(), context);
    assertEquals(1, requests.size());
    assertEquals(1, context.warnings.size());
    assertTrue(context.warnings.get(0), context.warnings.get(0).startsWith("unparseable data in client stream at byte 18"));
  }

  @Test
  public void testParseReturnsNullWithoutRequest() {
    HttpSessionDecoder decoder = new HttpSessionDecoder();
    assertNull(decoder.parse(org.apache.drill.exec.store.pcap.protocol.TcpStream.of(Collections.emptyList()),
        org.apache.drill.exec.store.pcap.protocol.TcpStream.of(Collections.emptyList()), new TestHttpParser.Context(false)));
  }

  @Test
  public void testInterimResponsesDoNotShiftPairing() {
    // 100 Continue is not the answer to the request; the final status is
    String client = "POST /upload HTTP/1.1\r\nExpect: 100-continue\r\nContent-Length: 2\r\n\r\nok"
        + "GET /next HTTP/1.1\r\n\r\n";
    String server = "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 201 Created\r\nContent-Length: 0\r\n\r\n"
        + "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    TestHttpParser.Context context = new TestHttpParser.Context(false);
    List<HttpMessage> requests = HttpSessionDecoder.parseStream(TestHttpParser.bytes(client), true,
        Collections.emptyList(), context);
    List<HttpMessage> responses = HttpSessionDecoder.parseStream(TestHttpParser.bytes(server), false, requests, context);
    assertEquals(2, responses.size());
    assertEquals(Integer.valueOf(201), responses.get(0).statusCode);
    assertEquals(Integer.valueOf(200), responses.get(1).statusCode);
  }
}
