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
package org.apache.drill.exec.store.pcap.protocol.quic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.TestPackets;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestQuicDecoder extends BaseTest {

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

  // A real QUIC v1 Initial carrying a TLS ClientHello (SNI example.org, ALPN h2), built by
  // quic_fixtures.py#build_initial with pad_to=0 and cross-checked by an independent Python decryptor.
  private static final String INITIAL_HEX =
        "ca00000001088394c8f03e51570803c0ffee004085fd50d9e146b451b79b7ca34ec0a54f94c48b73c6241718f7a15b1b16bf79af7f883514a11a62fc775e106b"
      + "44620827fb17e5e566eb116bd631e38076a4ddc08b3f691438cf283a95896feb930133ea7cf13cbd43afb032fe9df98853677fed442ca00c0284e626f2681c50"
      + "22369cfb105a2349f082a408d800abbee9964bbb67cfda4ece90";

  private final QuicDecoder decoder = new QuicDecoder();

  private static byte[] hex(String s) {
    byte[] out = new byte[s.length() / 2];
    for (int i = 0; i < out.length; i++) {
      out[i] = (byte) Integer.parseInt(s.substring(2 * i, 2 * i + 2), 16);
    }
    return out;
  }

  private QuicInitial parse(byte[] payload, Context context) {
    return decoder.parse(TestPackets.udp("10.0.0.1", 50000, "10.0.0.2", 443, payload), payload, context);
  }

  @Test
  public void testAcceptsUdpWithQuicPortsOrInitialFirstByte() {
    assertTrue(decoder.accepts(TestPackets.udp("10.0.0.1", 50000, "10.0.0.2", 443, hex(INITIAL_HEX))));
    assertTrue(decoder.accepts(TestPackets.udp("10.0.0.1", 60000, "10.0.0.2", 7777, new byte[] {(byte) 0xc3, 0})));
    assertFalse(decoder.accepts(TestPackets.udp("10.0.0.1", 60000, "10.0.0.2", 7777, new byte[] {0x40, 0})));
    assertFalse(decoder.accepts(TestPackets.tcp("10.0.0.1", 50000, "10.0.0.2", 443, 1, TestPackets.ACK, hex(INITIAL_HEX))));
  }

  @Test
  public void testInitialWithClientHello() {
    Context context = new Context();
    QuicInitial q = parse(hex(INITIAL_HEX), context);
    assertEquals("initial", q.packetType);
    assertEquals("00000001", q.version);
    assertEquals("8394c8f03e515708", q.dcid);
    assertEquals("c0ffee", q.scid);
    assertEquals("example.org", q.sni);
    assertEquals(Collections.singletonList("h2"), q.alpn);
    assertEquals(Collections.singletonList("TLS 1.3"), q.supportedVersions);
    assertEquals(Arrays.asList(0x1301, 0x1302, 0x1303), q.cipherSuites);
    assertEquals("q13d0305h2_55b375c5d22e_beb9f91c6f80", q.ja4);
    assertTrue(context.warnings.isEmpty());
  }

  @Test
  public void testShortHeaderIsNotDecoded() {
    assertNull(parse(new byte[] {0x40, 1, 2, 3, 4, 5, 6, 7}, new Context()));
  }

  @Test
  public void testNonQuicUdpIsNotDecoded() {
    assertNull(parse("not a quic packet at all".getBytes(StandardCharsets.US_ASCII), new Context()));
  }

  @Test
  public void testTruncatedInitialThrows() {
    try {
      parse(Arrays.copyOf(hex(INITIAL_HEX), 30), new Context());
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("exceeds the datagram"));
    }
  }

  @Test
  public void testTamperedInitialFailsAead() {
    byte[] tampered = hex(INITIAL_HEX);
    // Flip a byte in the ciphertext so the GCM tag no longer authenticates
    tampered[tampered.length - 20] ^= 0x01;
    try {
      parse(tampered, new Context());
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("AEAD authentication failed", e.getMessage());
    }
  }

  @Test
  public void testVersionNegotiation() {
    QuicInitial q = parse(hex("800000000004deadbeef000a0a0a0a"), new Context());
    assertEquals("version_negotiation", q.packetType);
    assertEquals("00000000", q.version);
    assertEquals("deadbeef", q.dcid);
  }

  @Test
  public void testOtherVersionReportedWithoutDecryption() {
    QuicInitial q = parse(hex("c3ff00001d04deadbeef03c0ffee"), new Context());
    assertNull(q.packetType);
    assertEquals("ff00001d", q.version);
    assertEquals("deadbeef", q.dcid);
    assertEquals("c0ffee", q.scid);
    assertNull(q.sni);
  }
}
