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
package org.apache.drill.exec.store.pcap.protocol.tls;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.TestPackets;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestTlsDecoder extends BaseTest {

  private static final class Context implements DecoderContext {
    final List<String> warnings = new ArrayList<>();

    @Override
    public boolean exposeCredentials() {
      return false;
    }

    @Override
    public void warn(String message) {
      warnings.add(message);
    }
  }

  private final TlsDecoder decoder = new TlsDecoder();

  private static byte[] concat(byte[]... parts) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] p : parts) {
      out.write(p, 0, p.length);
    }
    return out.toByteArray();
  }

  private static byte[] u16s(int... values) {
    ByteBuffer b = ByteBuffer.allocate(values.length * 2);
    for (int v : values) {
      b.putShort((short) v);
    }
    return b.array();
  }

  private static byte[] u8(int v) {
    return new byte[] {(byte) v};
  }

  private static byte[] ext(int type, byte[] data) {
    return concat(u16s(type, data.length), data);
  }

  private static byte[] sni(String host) {
    byte[] name = host.getBytes(StandardCharsets.US_ASCII);
    return ext(0, concat(u16s(name.length + 3), u8(0), u16s(name.length), name));
  }

  private static byte[] alpn(String... protocols) {
    ByteArrayOutputStream list = new ByteArrayOutputStream();
    for (String p : protocols) {
      list.write(p.length());
      list.write(p.getBytes(StandardCharsets.US_ASCII), 0, p.length());
    }
    return ext(16, concat(u16s(list.size()), list.toByteArray()));
  }

  private static byte[] u16List(int type, int... values) {
    return ext(type, concat(u16s(values.length * 2), u16s(values)));
  }

  private static byte[] pointFormats(int... formats) {
    byte[] b = new byte[formats.length + 1];
    b[0] = (byte) formats.length;
    for (int i = 0; i < formats.length; i++) {
      b[i + 1] = (byte) formats[i];
    }
    return ext(11, b);
  }

  private static byte[] clientVersions(int... versions) {
    return ext(43, concat(u8(versions.length * 2), u16s(versions)));
  }

  private static byte[] handshake(int type, byte[] body) {
    return concat(new byte[] {(byte) type, (byte) (body.length >> 16), (byte) (body.length >> 8), (byte) body.length},
        body);
  }

  private static byte[] record(int version, byte[] fragment) {
    return concat(u8(22), u16s(version, fragment.length), fragment);
  }

  private static byte[] clientHelloBody(int version, byte[] sessionId, int[] ciphers, byte[]... extensions) {
    byte[] exts = concat(extensions);
    return concat(u16s(version), new byte[32], u8(sessionId.length), sessionId,
        u16s(ciphers.length * 2), u16s(ciphers), u8(1), u8(0),
        extensions.length == 0 ? new byte[0] : concat(u16s(exts.length), exts));
  }

  private static byte[] clientHello(int version, byte[] sessionId, int[] ciphers, byte[]... extensions) {
    return record(0x0301, handshake(1, clientHelloBody(version, sessionId, ciphers, extensions)));
  }

  private static byte[] serverHello(int version, int cipher, byte[]... extensions) {
    byte[] exts = concat(extensions);
    byte[] body = concat(u16s(version), new byte[32], u8(4), new byte[] {1, 2, 3, 4}, u16s(cipher), u8(0),
        extensions.length == 0 ? new byte[0] : concat(u16s(exts.length), exts));
    return record(0x0303, handshake(2, body));
  }

  private TlsHello parse(byte[] payload, Context context) {
    Packet packet = TestPackets.tcp("10.0.0.1", 50000, "10.0.0.2", 443, 1, TestPackets.ACK | TestPackets.PSH, payload);
    return decoder.parse(packet, payload, context);
  }

  @Test
  public void testAcceptsTlsPortsOverTcpOnly() {
    assertTrue(decoder.accepts(TestPackets.tcp("10.0.0.1", 50000, "10.0.0.2", 443, 1, TestPackets.ACK, new byte[1])));
    assertTrue(decoder.accepts(TestPackets.tcp("10.0.0.2", 993, "10.0.0.1", 50000, 1, TestPackets.ACK, new byte[1])));
    assertTrue(decoder.accepts(TestPackets.tcp("10.0.0.1", 50000, "10.0.0.2", 8443, 1, TestPackets.ACK, new byte[1])));
    assertFalse(decoder.accepts(TestPackets.tcp("10.0.0.1", 50000, "10.0.0.2", 80, 1, TestPackets.ACK, new byte[1])));
    assertFalse(decoder.accepts(TestPackets.udp("10.0.0.1", 50000, "10.0.0.2", 443, new byte[1])));
  }

  @Test
  public void testPublishedJa3Example() {
    // The example from the JA3 README: 769,47-53-5-10-49161-49162-49171-49172-50-56-19-4,0-10-11,23-24-25,0
    Context context = new Context();
    TlsHello h = parse(clientHello(0x0301, new byte[0],
        new int[] {47, 53, 5, 10, 49161, 49162, 49171, 49172, 50, 56, 19, 4},
        sni("example.com"), u16List(10, 23, 24, 25), pointFormats(0)), context);
    assertEquals("client_hello", h.handshakeType);
    assertEquals("TLS 1.0", h.version);
    assertEquals("769,47-53-5-10-49161-49162-49171-49172-50-56-19-4,0-10-11,23-24-25,0", h.ja3);
    assertEquals("ada70206e40642a3e4461f35503241d5", h.ja3Hash);
    assertEquals("example.com", h.sni);
    assertEquals(Arrays.asList(0, 10, 11), h.extensions);
    assertTrue(context.warnings.isEmpty());
  }

  @Test
  public void testClientHelloWithGrease() {
    Context context = new Context();
    byte[] sessionId = new byte[32];
    sessionId[0] = (byte) 0xAB;
    sessionId[31] = 0x01;
    TlsHello h = parse(clientHello(0x0303, sessionId, new int[] {0x0a0a, 0x1301, 0x1302, 0xc02b},
        ext(0x1a1a, new byte[0]), sni("www.example.org"), alpn("h2", "http/1.1"),
        u16List(10, 0x2a2a, 29, 23), pointFormats(0), u16List(13, 0x0403, 0x0804),
        clientVersions(0x3a3a, 0x0304, 0x0303)), context);
    assertEquals("client_hello", h.handshakeType);
    assertEquals("TLS 1.0", h.recordVersion);
    assertEquals("TLS 1.2", h.version);
    assertEquals(Arrays.asList("0x3a3a", "TLS 1.3", "TLS 1.2"), h.supportedVersions);
    assertEquals("ab000000000000000000000000000000000000000000000000000000000000" + "01", h.sessionId);
    assertEquals("www.example.org", h.sni);
    assertEquals(Arrays.asList("h2", "http/1.1"), h.alpn);
    assertEquals(Arrays.asList(0x0a0a, 0x1301, 0x1302, 0xc02b), h.cipherSuites);
    assertEquals(Arrays.asList(0x1a1a, 0, 16, 10, 11, 13, 43), h.extensions);
    assertEquals(Arrays.asList(0x2a2a, 29, 23), h.supportedGroups);
    assertEquals(Collections.singletonList(0), h.ecPointFormats);
    assertEquals(Arrays.asList(0x0403, 0x0804), h.signatureAlgorithms);
    assertEquals("771,4865-4866-49195,0-16-10-11-13-43,29-23,0", h.ja3);
    assertEquals(32, h.ja3Hash.length());
    assertNull(h.cipherSuite);
    assertNull(h.ja3s);
    assertTrue(context.warnings.isEmpty());
  }

  @Test
  public void testClientHelloWithoutExtensions() {
    TlsHello h = parse(clientHello(0x0301, new byte[0], new int[] {4, 5}), new Context());
    assertEquals("769,4-5,,,", h.ja3);
    assertTrue(h.extensions.isEmpty());
    assertNull(h.sni);
  }

  @Test
  public void testServerHello() {
    Context context = new Context();
    TlsHello h = parse(serverHello(0x0303, 0x1301, ext(43, u16s(0x0304)), ext(51, new byte[] {0, 29, 0, 0})),
        context);
    assertEquals("server_hello", h.handshakeType);
    assertEquals("TLS 1.2", h.version);
    assertEquals(Collections.singletonList("TLS 1.3"), h.supportedVersions);
    assertEquals(Integer.valueOf(0x1301), h.cipherSuite);
    assertEquals("01020304", h.sessionId);
    assertEquals(Arrays.asList(43, 51), h.extensions);
    assertEquals("771,4865,43-51", h.ja3s);
    assertEquals("f4febc55ea12b31ae17cfb7e614afda8", h.ja3sHash);
    assertNull(h.ja3);
    assertNull(h.cipherSuites);
    assertTrue(context.warnings.isEmpty());
  }

  @Test
  public void testServerHelloAlpnAndEmptySni() {
    TlsHello h = parse(serverHello(0x0303, 0xc02f, ext(0, new byte[0]), alpn("h2")), new Context());
    assertNull(h.sni);
    assertEquals(Collections.singletonList("h2"), h.alpn);
    assertEquals("771,49199,0-16", h.ja3s);
  }

  @Test
  public void testNotTls() {
    Context context = new Context();
    byte[] hello = clientHello(0x0303, new byte[0], new int[] {0x1301}, sni("a.example"));
    assertNull(parse("GET / HTTP/1.1\r\nHost: a\r\n\r\n".getBytes(StandardCharsets.US_ASCII), context));
    assertNull(parse(new byte[] {22, 3, 1}, context));
    byte[] appData = hello.clone();
    appData[0] = 23;
    assertNull(parse(appData, context));
    byte[] badVersion = hello.clone();
    badVersion[2] = 5;
    assertNull(parse(badVersion, context));
    byte[] certificate = hello.clone();
    certificate[5] = 11;
    assertNull(parse(certificate, context));
    byte[] zeroLength = hello.clone();
    zeroLength[3] = 0;
    zeroLength[4] = 0;
    assertNull(parse(zeroLength, context));
    byte[] badHelloVersion = hello.clone();
    badHelloVersion[9] = 7;
    assertNull(parse(badHelloVersion, context));
    assertTrue(context.warnings.isEmpty());
  }

  @Test
  public void testHelloContinuesInLaterSegment() {
    Context context = new Context();
    byte[] hello = clientHello(0x0303, new byte[0], new int[] {0x1301, 0x1302, 0x1303}, sni("split.example"),
        alpn("h2"));
    // Cut in the middle of the ALPN extension: the SNI was already seen
    byte[] first = Arrays.copyOf(hello, hello.length - 3);
    TlsHello h = parse(first, context);
    assertEquals("client_hello", h.handshakeType);
    assertEquals("split.example", h.sni);
    assertEquals(Arrays.asList(0x1301, 0x1302, 0x1303), h.cipherSuites);
    assertNull(h.alpn);
    assertNull(h.ja3);
    assertEquals(Collections.singletonList("handshake continues in a later segment"), context.warnings);
  }

  @Test
  public void testOnlyHandshakeTypeInSegment() {
    Context context = new Context();
    byte[] hello = clientHello(0x0303, new byte[0], new int[] {0x1301});
    TlsHello h = parse(Arrays.copyOf(hello, 7), context);
    assertEquals("client_hello", h.handshakeType);
    assertNull(h.version);
    assertEquals(Collections.singletonList("handshake continues in a later segment"), context.warnings);
  }

  @Test
  public void testTruncatedHelloThrows() {
    // The lengths say the hello is complete, but the session ID length overruns it
    byte[] body = clientHelloBody(0x0303, new byte[0], new int[] {0x1301});
    body[34] = 32;
    try {
      parse(record(0x0301, handshake(1, body)), new Context());
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().startsWith("truncated client_hello"));
    }
  }

  @Test
  public void testBadExtensionLengthThrows() {
    byte[] hello = clientHello(0x0303, new byte[0], new int[] {0x1301}, sni("a.example"));
    // Make the server_name list length larger than the extension
    int listLength = hello.length - 14;
    hello[listLength] = 0x7F;
    try {
      parse(hello, new Context());
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("server_name"));
    }
  }

  @Test
  public void testListsAreCapped() {
    Context context = new Context();
    int[] ciphers = new int[100];
    StringBuilder expected = new StringBuilder("771,");
    for (int i = 0; i < ciphers.length; i++) {
      ciphers[i] = i + 1;
      expected.append(i == 0 ? "" : "-").append(i + 1);
    }
    TlsHello h = parse(clientHello(0x0303, new byte[0], ciphers), context);
    assertEquals(64, h.cipherSuites.size());
    // JA3 uses the whole list
    assertEquals(expected.append(",,,").toString(), h.ja3);
    assertEquals(Collections.singletonList("cipher_suites truncated to 64"), context.warnings);
  }
}
