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
package org.apache.drill.exec.store.pcap.protocol.sip;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.TestPackets;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestSipDecoder extends BaseTest {

  private static final class Context implements DecoderContext {
    final List<String> warnings = new ArrayList<>();

    @Override
    public boolean exposeCredentials() {
      return true;
    }

    @Override
    public void warn(String message) {
      warnings.add(message);
    }
  }

  private static SipMessage parse(String text, Context context) {
    return SipParser.parse(text.getBytes(StandardCharsets.UTF_8), context);
  }

  static final String INVITE = "INVITE sip:bob@biloxi.example.com SIP/2.0\r\n"
      + "Via: SIP/2.0/UDP pc33.atlanta.example.com;branch=z9hG4bK776asdhds, SIP/2.0/UDP proxy.example.com;branch=z9hG4bK1\r\n"
      + "Via: SIP/2.0/UDP edge.example.com;branch=z9hG4bK2\r\n"
      + "Max-Forwards: 70\r\n"
      + "To: Bob <sip:bob@biloxi.example.com>\r\n"
      + "From: \"Alice, A.\" <sip:alice@atlanta.example.com>;tag=1928301774\r\n"
      + "Call-ID: a84b4c76e66710@pc33.atlanta.example.com\r\n"
      + "CSeq: 314159 INVITE\r\n"
      + "Contact: <sip:alice@pc33.atlanta.example.com>\r\n"
      + "User-Agent: softphone/1.0\r\n"
      + "Authorization: Digest username=\"alice\", realm=\"atlanta.example.com\",\r\n"
      + " nonce=\"84a4cc6f3082121f32b42a2187831a9e\", response=\"7587245234b3434cc3412213e5f113a5\"\r\n"
      + "Content-Type: application/sdp\r\n"
      + "Content-Length: 4\r\n"
      + "\r\n"
      + "v=0\n";

  @Test
  public void testInvite() {
    SipMessage m = parse(INVITE, new Context());
    assertTrue(m.isRequest);
    assertEquals("INVITE", m.method);
    assertEquals("sip:bob@biloxi.example.com", m.requestUri);
    assertNull(m.statusCode);
    assertEquals("Bob <sip:bob@biloxi.example.com>", m.to);
    assertEquals("\"Alice, A.\" <sip:alice@atlanta.example.com>;tag=1928301774", m.from);
    assertEquals("a84b4c76e66710@pc33.atlanta.example.com", m.callId);
    assertEquals("314159 INVITE", m.cseq);
    assertEquals("<sip:alice@pc33.atlanta.example.com>", m.contact);
    assertEquals("softphone/1.0", m.userAgent);
    assertEquals(Arrays.asList("SIP/2.0/UDP pc33.atlanta.example.com;branch=z9hG4bK776asdhds",
        "SIP/2.0/UDP proxy.example.com;branch=z9hG4bK1", "SIP/2.0/UDP edge.example.com;branch=z9hG4bK2"), m.via);
    assertEquals("application/sdp", m.contentType);
    assertEquals(4L, (long) m.contentLength);
    assertEquals("alice", m.username);
    assertFalse(m.passwordPresent);
    assertEquals(12, m.headers.size());
    assertTrue(m.headers.get(9)[1].endsWith("response=\"7587245234b3434cc3412213e5f113a5\""));
  }

  @Test
  public void testCompactResponse() {
    SipMessage m = parse("SIP/2.0 180 Ringing\r\nv: SIP/2.0/UDP host;branch=z9hG4bK1\r\nf: <sip:a@x>;tag=1\r\n"
        + "t: <sip:b@y>;tag=2\r\ni: abc@x\r\nCSeq: 1 INVITE\r\nm: <sip:b@10.0.0.2>\r\nc: text/plain\r\nl: 0\r\n\r\n",
        new Context());
    assertFalse(m.isRequest);
    assertNull(m.method);
    assertEquals(180, (int) m.statusCode);
    assertEquals("Ringing", m.reason);
    assertEquals("<sip:a@x>;tag=1", m.from);
    assertEquals("<sip:b@y>;tag=2", m.to);
    assertEquals("abc@x", m.callId);
    assertEquals("<sip:b@10.0.0.2>", m.contact);
    assertEquals("text/plain", m.contentType);
    assertEquals(0L, (long) m.contentLength);
    assertEquals(Arrays.asList("SIP/2.0/UDP host;branch=z9hG4bK1"), m.via);
    assertNull(m.username);
  }

  @Test
  public void testProxyAuthorizationAndBadContentLength() {
    Context context = new Context();
    SipMessage m = parse("REGISTER sips:registrar.example.com SIP/2.0\r\nProxy-Authorization: Digest username=bob, realm=x\r\n"
        + "Content-Length: lots\r\n\r\n", context);
    assertEquals("REGISTER", m.method);
    assertEquals("bob", m.username);
    assertNull(m.contentLength);
    assertEquals(1, context.warnings.size());
  }

  @Test
  public void testNotSip() {
    assertNull(parse("GET / HTTP/1.1\r\nHost: x\r\n\r\n", new Context()));
    assertNull(parse("FOO sip:x SIP/2.0\r\n\r\n", new Context()));
    assertNull(parse("INVITE sip:x SIP/3.0\r\n\r\n", new Context()));
    assertNull(parse("\r\n\r\n", new Context()));
    assertNull(SipParser.parse(new byte[0], new Context()));
  }

  @Test
  public void testMalformedHeader() {
    try {
      parse("OPTIONS sip:x@y SIP/2.0\r\nVia: SIP/2.0/UDP h\r\ngarbage\r\n\r\n", new Context());
      fail("expected malformed SIP");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("malformed header line 2"));
    }
  }

  @Test
  public void testDecoderAccepts() {
    SipDecoder decoder = new SipDecoder();
    byte[] data = INVITE.getBytes(StandardCharsets.UTF_8);
    assertTrue(decoder.accepts(TestPackets.udp("10.0.0.1", 5060, "10.0.0.2", 5060, data)));
    assertTrue(decoder.accepts(TestPackets.tcp("10.0.0.1", 40000, "10.0.0.2", 5060, 1, TestPackets.ACK, data)));
    assertFalse(decoder.accepts(TestPackets.udp("10.0.0.1", 40000, "10.0.0.2", 5061, data)));
  }
}
