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
import static org.junit.Assert.assertNull;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

/**
 * JA4 TLS client fingerprint, cross-checked against an independent reference written from the FoxIO JA4
 * specification (see fixtures/tls_fixtures.py#ja4_from_spec and the hand computation in each case below).
 */
public class TestJa4 extends BaseTest {

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

  private static byte[] handshakeBody(int version, int[] ciphers, byte[]... extensions) {
    byte[] exts = concat(extensions);
    return concat(u16s(version), new byte[32], u8(0),
        u16s(ciphers.length * 2), u16s(ciphers), u8(1), u8(0),
        extensions.length == 0 ? new byte[0] : concat(u16s(exts.length), exts));
  }

  private static byte[] handshake(int type, byte[] body) {
    return concat(new byte[] {(byte) type, (byte) (body.length >> 16), (byte) (body.length >> 8), (byte) body.length},
        body);
  }

  private static byte[] record(byte[] fragment) {
    return concat(u8(22), u16s(0x0301, fragment.length), fragment);
  }

  @Test
  public void testJa4WithSniAlpnAndSignatureAlgorithms() {
    // legacy version TLS 1.2, ciphers 1301,1302, extensions sni + alpn(h2) + signature_algorithms(0403,0804)
    // a = t 12 d 02 (ciphers) 03 (extensions, counting sni + alpn) h2
    // b = sha256("1301,1302")[:12]; c = sha256("000d_0403,0804")[:12]  (sni/alpn removed from c)
    Context context = new Context();
    byte[] hello = record(handshake(1, handshakeBody(0x0303, new int[] {0x1301, 0x1302},
        sni("example.com"), alpn("h2"), u16List(13, 0x0403, 0x0804))));
    TlsHello h = TlsParser.parse(hello, context);
    assertEquals("t12d0203h2_62ed6f6ca7ad_ef95ca21a004", h.ja4);
    assertEquals("t12d0203h2_1301,1302_000d_0403,0804", h.ja4Raw);
  }

  @Test
  public void testJa4WithoutSniAlpnOrSignatureAlgorithms() {
    // legacy version TLS 1.0, ciphers 4,5, extensions supported_groups(10) + ec_point_formats(11)
    // a = t 10 i 02 02 00 (no alpn); b = sha256("0004,0005")[:12]; c = sha256("000a,000b")[:12] (no sig algs)
    Context context = new Context();
    byte[] hello = record(handshake(1, handshakeBody(0x0301, new int[] {4, 5},
        u16List(10, 29), pointFormats(0))));
    TlsHello h = TlsParser.parse(hello, context);
    assertEquals("t10i020200_6e254592683c_33a13ba74d1c", h.ja4);
    assertEquals("t10i020200_0004,0005_000a,000b", h.ja4Raw);
  }

  @Test
  public void testJa4VersionFromSupportedVersionsAndGreaseIgnored() {
    // GREASE cipher, GREASE extension and GREASE supported_version are all excluded; version is 1.3
    Context context = new Context();
    byte[] hello = record(handshake(1, handshakeBody(0x0303, new int[] {0x0a0a, 0x1301, 0x1302},
        ext(0x1a1a, new byte[0]), sni("a.example"), alpn("h2"),
        ext(43, concat(u8(6), u16s(0x3a3a, 0x0304, 0x0303))))));
    TlsHello h = TlsParser.parse(hello, context);
    // version 13, SNI present, 2 non-GREASE ciphers, 3 non-GREASE extensions (sni + alpn + supported_versions)
    assertEquals("t13d0203h2", h.ja4.substring(0, 10));
  }

  @Test
  public void testJa4QuicFormFromBareHandshake() {
    // The same ClientHello as the first test, but parsed as a bare QUIC CRYPTO handshake: only the transport
    // character changes to 'q'. b and c are identical.
    Context context = new Context();
    byte[] message = handshake(1, handshakeBody(0x0303, new int[] {0x1301, 0x1302},
        sni("example.com"), alpn("h2"), u16List(13, 0x0403, 0x0804)));
    TlsHello h = TlsParser.parseHandshake(message, 0, true, context);
    assertEquals("q12d0203h2_62ed6f6ca7ad_ef95ca21a004", h.ja4);
  }

  @Test
  public void testServerHelloHasNoJa4() {
    Context context = new Context();
    byte[] body = concat(u16s(0x0303), new byte[32], u8(0), u16s(0x1301), u8(0),
        concat(u16s(ext(43, u16s(0x0304)).length), ext(43, u16s(0x0304))));
    byte[] hello = record(handshake(2, body));
    TlsHello h = TlsParser.parse(hello, context);
    assertEquals("server_hello", h.handshakeType);
    assertNull(h.ja4);
    assertNull(h.ja4Raw);
  }
}
