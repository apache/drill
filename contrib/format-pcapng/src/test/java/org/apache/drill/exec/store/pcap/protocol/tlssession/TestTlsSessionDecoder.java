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
package org.apache.drill.exec.store.pcap.protocol.tlssession;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestTlsSessionDecoder extends BaseTest {

  static final class Context implements DecoderContext {
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

  // ------------------------------------------------------------ byte builders

  static byte[] cat(byte[]... parts) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] p : parts) {
      out.write(p, 0, p.length);
    }
    return out.toByteArray();
  }

  static byte[] u8(byte[] b) {
    return cat(new byte[] {(byte) b.length}, b);
  }

  static byte[] u16(byte[] b) {
    return cat(new byte[] {(byte) (b.length >> 8), (byte) b.length}, b);
  }

  static byte[] u24(byte[] b) {
    return cat(new byte[] {(byte) (b.length >> 16), (byte) (b.length >> 8), (byte) b.length}, b);
  }

  static byte[] short16(int... values) {
    byte[] out = new byte[values.length * 2];
    for (int i = 0; i < values.length; i++) {
      out[2 * i] = (byte) (values[i] >> 8);
      out[2 * i + 1] = (byte) values[i];
    }
    return out;
  }

  static byte[] ext(int type, byte[] data) {
    return cat(short16(type), u16(data));
  }

  static byte[] sni(String host) {
    return ext(0, u16(cat(new byte[] {0}, u16(host.getBytes(StandardCharsets.US_ASCII)))));
  }

  static byte[] alpn(String... protocols) {
    ByteArrayOutputStream list = new ByteArrayOutputStream();
    for (String p : protocols) {
      byte[] b = u8(p.getBytes(StandardCharsets.US_ASCII));
      list.write(b, 0, b.length);
    }
    return ext(16, u16(list.toByteArray()));
  }

  static byte[] handshake(int type, byte[] body) {
    return cat(new byte[] {(byte) type}, u24(body));
  }

  static byte[] record(int contentType, byte[] payload) {
    return cat(new byte[] {(byte) contentType, 3, 3}, u16(payload));
  }

  static byte[] clientHello(byte[] sid, int[] suites, byte[]... extensions) {
    return handshake(1, cat(short16(0x0303), new byte[32], u8(sid), u16(short16(suites)), u8(new byte[] {0}),
        u16(cat(extensions))));
  }

  static byte[] serverHello(byte[] sid, int suite, byte[]... extensions) {
    return serverHello(new byte[32], sid, suite, extensions);
  }

  static byte[] serverHello(byte[] random, byte[] sid, int suite, byte[]... extensions) {
    return handshake(2, cat(short16(0x0303), random, u8(sid), short16(suite), new byte[] {0}, u16(cat(extensions))));
  }

  static byte[] certificate(byte[]... ders) {
    ByteArrayOutputStream list = new ByteArrayOutputStream();
    for (byte[] d : ders) {
      byte[] b = u24(d);
      list.write(b, 0, b.length);
    }
    return handshake(11, u24(list.toByteArray()));
  }

  static byte[] resource(String name) {
    try (InputStream in = TestTlsSessionDecoder.class.getResourceAsStream("/decoders/tls/" + name)) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buf = new byte[4096];
      for (int n = in.read(buf); n > 0; n = in.read(buf)) {
        out.write(buf, 0, n);
      }
      return out.toByteArray();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  static byte[] sid(int start) {
    byte[] b = new byte[32];
    for (int i = 0; i < b.length; i++) {
      b[i] = (byte) (start + i);
    }
    return b;
  }

  static TlsHandshake parse(byte[] client, byte[] server, Context context) {
    return TlsSessionDecoder.parseStreams(client, -1, server, -1, context);
  }

  // ------------------------------------------------------------ tests

  @Test
  public void testTls12WithCertificateChain() {
    byte[] leaf = resource("leaf.der");
    byte[] ca = resource("ca.der");
    byte[] client = cat(record(22, clientHello(new byte[0], new int[] {0xC02F, 0x009C}, sni("www.example.com"),
        alpn("h2", "http/1.1"))), record(20, new byte[] {1}), record(22, new byte[40]));
    // Certificate fragmented across two records
    byte[] cert = certificate(leaf, ca);
    int half = cert.length / 2;
    byte[] server = cat(record(22, serverHello(sid(100), 0xC02F, alpn("h2"))),
        record(22, Arrays.copyOfRange(cert, 0, half)),
        record(22, cat(Arrays.copyOfRange(cert, half, cert.length), handshake(14, new byte[0]))),
        record(20, new byte[] {1}), record(22, new byte[40]));
    Context context = new Context();
    TlsHandshake h = parse(client, server, context);
    assertEquals(context.warnings.toString(), 0, context.warnings.size());
    assertEquals("TLS 1.2", h.clientVersion);
    assertEquals("TLS 1.2", h.serverVersion);
    assertEquals("www.example.com", h.sni);
    assertEquals(Arrays.asList("h2", "http/1.1"), h.alpnOffered);
    assertEquals("h2", h.alpnSelected);
    assertEquals(Integer.valueOf(0xC02F), h.cipherSuite);
    assertEquals("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256", h.cipherSuiteName);
    assertEquals(Boolean.FALSE, h.sessionResumed);
    assertEquals(Boolean.FALSE, h.certificateEncrypted);
    assertEquals(Integer.valueOf(2), h.certificateCount);
    assertEquals(2, h.certificates.size());

    TlsCertificate c = h.certificates.get(0);
    assertEquals("CN=www.example.com,O=Example", c.subject);
    assertEquals("CN=Drill Test Root CA,O=Apache Drill", c.issuer);
    assertEquals("0a1b2c3d4e5f", c.serial);
    assertEquals(Instant.parse("2024-01-02T03:04:05Z"), c.notBefore);
    assertEquals(Instant.parse("2025-01-02T03:04:05Z"), c.notAfter);
    assertEquals(Arrays.asList("www.example.com", "example.com", "93.184.216.34"), c.subjectAltNames);
    assertEquals("SHA256withRSA", c.signatureAlgorithm);
    assertEquals("EC", c.publicKeyAlgorithm);
    assertEquals(Integer.valueOf(256), c.publicKeyBits);
    assertEquals("8fd6d21ce7848700f6b269ae48215fb340f648f38db30c42be3a6d1d01ca881a", c.sha256);
    assertEquals(Boolean.FALSE, c.isSelfSigned);

    TlsCertificate root = h.certificates.get(1);
    assertEquals("1001", root.serial);
    assertEquals("RSA", root.publicKeyAlgorithm);
    assertEquals(Integer.valueOf(2048), root.publicKeyBits);
    assertEquals(Boolean.TRUE, root.isSelfSigned);
    assertEquals("8dddae1f78b907846f93907594d165139f3eed06ca73ca2e198330809f9501a4", root.sha256);
  }

  @Test
  public void testTls13CertificateIsEncrypted() {
    byte[] id = sid(200);
    byte[] client = record(22, clientHello(id, new int[] {0x1301, 0x1302}, sni("tls13.example.org"),
        ext(43, u8(short16(0x0A0A, 0x0304, 0x0303)))));
    byte[] server = cat(record(22, serverHello(id, 0x1301, ext(43, short16(0x0304)))),
        record(20, new byte[] {1}), record(23, new byte[200]));
    Context context = new Context();
    TlsHandshake h = parse(client, server, context);
    assertTrue(context.warnings.isEmpty());
    assertEquals("TLS 1.2", h.clientVersion);
    assertEquals(Arrays.asList("TLS 1.3", "TLS 1.2"), h.clientSupportedVersions);
    assertEquals("TLS 1.3", h.serverVersion);
    assertEquals("TLS_AES_128_GCM_SHA256", h.cipherSuiteName);
    // The echoed legacy session id is not a resumption in TLS 1.3
    assertEquals(Boolean.FALSE, h.sessionResumed);
    assertEquals(Boolean.TRUE, h.certificateEncrypted);
    assertTrue(h.certificates.isEmpty());
    assertNull(h.certificateCount);
  }

  @Test
  public void testTls13PskResumption() {
    byte[] client = record(22, clientHello(sid(1), new int[] {0x1301}, ext(43, u8(short16(0x0304)))));
    byte[] server = record(22, serverHello(sid(1), 0x1301, ext(43, short16(0x0304)), ext(41, short16(0))));
    TlsHandshake h = parse(client, server, new Context());
    assertEquals(Boolean.TRUE, h.sessionResumed);
  }

  @Test
  public void testTls12Resumption() {
    byte[] client = record(22, clientHello(sid(7), new int[] {0xC02F}));
    byte[] server = cat(record(22, serverHello(sid(7), 0xC02F)), record(20, new byte[] {1}));
    TlsHandshake h = parse(client, server, new Context());
    assertEquals(Boolean.TRUE, h.sessionResumed);
    assertNull(h.certificateCount);
    assertEquals(Boolean.FALSE, h.certificateEncrypted);
  }

  @Test
  public void testHelloRetryRequest() {
    byte[] hrrRandom = TlsParser.HELLO_RETRY_RANDOM;
    byte[] client = cat(record(22, clientHello(sid(1), new int[] {0x1302}, sni("a.example"),
        ext(43, u8(short16(0x0304))))), record(20, new byte[] {1}),
        record(22, clientHello(sid(1), new int[] {0x1302}, sni("a.example"), ext(43, u8(short16(0x0304))))),
        record(23, new byte[10]));
    byte[] server = cat(record(22, serverHello(hrrRandom, sid(1), 0x1302, ext(43, short16(0x0304)))),
        record(20, new byte[] {1}), record(22, serverHello(sid(1), 0x1302, ext(43, short16(0x0304)))),
        record(23, new byte[10]));
    Context context = new Context();
    TlsHandshake h = parse(client, server, context);
    assertTrue(context.warnings.toString(), context.warnings.isEmpty());
    assertEquals(Boolean.TRUE, h.helloRetryRequest);
    assertEquals("TLS 1.3", h.serverVersion);
    assertEquals("TLS_AES_256_GCM_SHA384", h.cipherSuiteName);
    assertEquals("a.example", h.sni);
  }

  @Test
  public void testNotTls() {
    byte[] http = "GET / HTTP/1.1\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    assertNull(parse(http, new byte[0], new Context()));
    assertNull(parse(new byte[0], new byte[0], new Context()));
    // A handshake record that does not hold a ClientHello
    assertNull(parse(record(22, handshake(2, new byte[40])), new byte[0], new Context()));
  }

  @Test
  public void testServerOnly() {
    byte[] server = record(22, serverHello(sid(1), 0x1234));
    TlsHandshake h = parse(new byte[0], server, new Context());
    assertEquals("0x1234", h.cipherSuiteName);
    assertNull(h.sessionResumed);
  }

  @Test
  public void testMalformedClientHelloThrows() {
    byte[] hello = clientHello(new byte[0], new int[] {0xC02F}, sni("bad.example.com"));
    // Extensions length claims more bytes than the message has
    int at = hello.length - sni("bad.example.com").length - 2;
    hello[at] = 4;
    hello[at + 1] = 0;
    try {
      parse(record(22, hello), new byte[0], new Context());
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().startsWith("malformed ClientHello"));
    }
  }

  @Test
  public void testMalformedServerMessageWarns() {
    byte[] client = record(22, clientHello(new byte[0], new int[] {0xC02F}, sni("x.example")));
    byte[] server = record(22, handshake(2, new byte[5]));
    Context context = new Context();
    TlsHandshake h = parse(client, server, context);
    assertEquals("x.example", h.sni);
    assertEquals(1, context.warnings.size());
    assertTrue(context.warnings.get(0), context.warnings.get(0).startsWith("malformed ServerHello"));
  }

  @Test
  public void testUnparseableCertificateKeepsFingerprint() {
    byte[] client = record(22, clientHello(new byte[0], new int[] {0xC02F}));
    byte[] server = cat(record(22, serverHello(sid(1), 0xC02F)), record(22, certificate(new byte[] {1, 2, 3})));
    Context context = new Context();
    TlsHandshake h = parse(client, server, context);
    assertEquals(1, h.certificates.size());
    assertEquals("039058c6f2c0cb492c533b0a4d14ef77cc0f78abccced5287d84a1a2011cfb81", h.certificates.get(0).sha256);
    assertNull(h.certificates.get(0).subject);
    assertEquals(1, context.warnings.size());
    assertTrue(context.warnings.get(0), context.warnings.get(0).startsWith("certificate 0 could not be parsed"));
  }

  @Test
  public void testCertificateListIsCapped() {
    byte[][] ders = new byte[12][];
    for (int i = 0; i < ders.length; i++) {
      ders[i] = new byte[] {(byte) i};
    }
    byte[] client = record(22, clientHello(new byte[0], new int[] {0xC02F}));
    byte[] server = cat(record(22, serverHello(sid(1), 0xC02F)), record(22, certificate(ders)));
    Context context = new Context();
    TlsHandshake h = parse(client, server, context);
    assertEquals(Integer.valueOf(12), h.certificateCount);
    assertEquals(TlsParser.MAX_CERTIFICATES, h.certificates.size());
    assertTrue(context.warnings.toString(), context.warnings.contains("kept the first 10 of 12 certificates"));
  }

  @Test
  public void testHandshakeDataIsBounded() {
    byte[] client = record(22, clientHello(new byte[0], new int[] {0xC02F}));
    // Records of 16 KB each, all part of one huge Certificate message
    byte[] server = record(22, serverHello(sid(1), 0xC02F));
    byte[] header = handshake(11, new byte[0]);
    header[1] = 0x10; // 1 MB message
    server = cat(server, record(22, header));
    for (int i = 0; i < 6; i++) {
      server = cat(server, record(22, new byte[16384]));
    }
    Context context = new Context();
    TlsHandshake h = parse(client, server, context);
    assertEquals("TLS 1.2", h.serverVersion);
    assertEquals(context.warnings.toString(), 1, context.warnings.size());
    assertTrue(context.warnings.get(0), context.warnings.get(0).startsWith("handshake message of 1048576 bytes"));
  }

  @Test
  public void testGapWarns() {
    byte[] client = record(22, clientHello(new byte[0], new int[] {0xC02F}, sni("gap.example")));
    byte[] server = record(22, serverHello(sid(1), 0xC02F));
    byte[] partial = Arrays.copyOf(server, 20);
    Context context = new Context();
    TlsHandshake h = TlsSessionDecoder.parseStreams(client, -1, partial, 20, context);
    assertEquals("gap.example", h.sni);
    assertNull(h.serverVersion);
    assertEquals(Arrays.asList("stopped at missing data in server stream at byte 20"), context.warnings);
  }

  @Test
  public void testTruncatedStreamWarns() {
    byte[] client = record(22, clientHello(new byte[0], new int[] {0xC02F}, sni("cut.example")));
    byte[] server = record(22, serverHello(sid(1), 0xC02F));
    Context context = new Context();
    TlsHandshake h = parse(client, Arrays.copyOf(server, 20), context);
    assertEquals("cut.example", h.sni);
    assertEquals(Arrays.asList("server stream ends inside a handshake record"), context.warnings);
  }
}
