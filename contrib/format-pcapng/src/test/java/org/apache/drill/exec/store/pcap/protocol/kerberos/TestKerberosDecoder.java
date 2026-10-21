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
package org.apache.drill.exec.store.pcap.protocol.kerberos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.TestPackets;
import org.apache.drill.exec.store.pcapng.PacketDecoder;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestKerberosDecoder extends BaseTest {

  private static final class Context implements DecoderContext {
    final List<String> warnings = new ArrayList<>();
    final boolean expose;

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

  private final KerberosDecoder decoder = new KerberosDecoder();

  // ---- BER builders ----

  private static byte[] tlv(int tag, byte[]... parts) {
    ByteArrayOutputStream body = new ByteArrayOutputStream();
    for (byte[] p : parts) {
      body.write(p, 0, p.length);
    }
    byte[] content = body.toByteArray();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(tag);
    if (content.length < 128) {
      out.write(content.length);
    } else {
      out.write(0x82);
      out.write(content.length >> 8);
      out.write(content.length & 0xFF);
    }
    out.write(content, 0, content.length);
    return out.toByteArray();
  }

  private static byte[] integer(long v) {
    return tlv(0x02, java.math.BigInteger.valueOf(v).toByteArray());
  }

  private static byte[] gstring(String s) {
    return tlv(0x1B, s.getBytes(StandardCharsets.UTF_8));
  }

  private static byte[] gtime(String s) {
    return tlv(0x18, s.getBytes(StandardCharsets.US_ASCII));
  }

  private static byte[] octets(byte[] b) {
    return tlv(0x04, b);
  }

  private static byte[] ctx(int n, byte[]... parts) {
    return tlv(0xA0 | n, parts);
  }

  private static byte[] app(int n, byte[]... parts) {
    return tlv(0x60 | n, parts);
  }

  private static byte[] seq(byte[]... parts) {
    return tlv(0x30, parts);
  }

  /** PrincipalName with the given components joined, name-type 1. */
  private static byte[] principal(int type, String... components) {
    byte[][] strings = new byte[components.length][];
    for (int i = 0; i < components.length; i++) {
      strings[i] = gstring(components[i]);
    }
    return seq(ctx(0, integer(type)), ctx(1, seq(strings)));
  }

  private static byte[] encryptedData(int etype) {
    return seq(ctx(0, integer(etype)), ctx(2, octets(new byte[] {1, 2, 3, 4})));
  }

  private byte[] asReq(boolean withPadata) {
    List<byte[]> top = new ArrayList<>();
    top.add(ctx(1, integer(5)));   // pvno
    top.add(ctx(2, integer(10)));  // msg-type AS-REQ
    if (withPadata) {
      top.add(ctx(3, seq(seq(ctx(1, integer(2)), ctx(2, octets(new byte[] {0}))))));
    }
    byte[] body = seq(
        ctx(0, tlv(0x03, new byte[] {0, 0, 0, 0, 0})),     // kdc-options BIT STRING
        ctx(1, principal(1, "alice")),                      // cname
        ctx(2, gstring("EXAMPLE.COM")),                     // realm
        ctx(3, principal(2, "krbtgt", "EXAMPLE.COM")),      // sname
        ctx(5, gtime("20240102030405Z")),                   // till
        ctx(8, seq(integer(18), integer(17), integer(23)))); // etype
    top.add(ctx(4, body));
    return app(10, seq(top.toArray(new byte[0][])));
  }

  private byte[] asRep() {
    byte[] ticket = ctx(5, app(1, seq(
        ctx(0, integer(5)),
        ctx(1, gstring("EXAMPLE.COM")),
        ctx(2, principal(2, "HTTP", "web.example.com")),
        ctx(3, encryptedData(23)))));
    return app(11, seq(
        ctx(0, integer(5)),                      // pvno
        ctx(1, integer(11)),                     // msg-type AS-REP
        ctx(3, gstring("EXAMPLE.COM")),          // crealm
        ctx(4, principal(1, "alice")),           // cname
        ticket,                                  // ticket [5]
        ctx(6, encryptedData(18))));             // enc-part (opaque)
  }

  private byte[] krbError() {
    return app(30, seq(
        ctx(0, integer(5)),                              // pvno
        ctx(1, integer(30)),                             // msg-type
        ctx(4, gtime("20240102030405Z")),               // stime
        ctx(5, integer(123)),                            // susec
        ctx(6, integer(25)),                             // error-code PREAUTH_REQUIRED
        ctx(9, gstring("EXAMPLE.COM")),                  // realm (service)
        ctx(10, principal(2, "krbtgt", "EXAMPLE.COM")))); // sname
  }

  private KerberosMessage parse(int port, byte[] payload, DecoderContext context) {
    PacketDecoder packet = TestPackets.udp("10.0.0.1", 40000, "10.0.0.2", port, payload);
    assertTrue(decoder.accepts(packet));
    return decoder.parse(packet, payload, context);
  }

  @Test
  public void testAsReq() {
    KerberosMessage m = parse(88, asReq(true), new Context(false));
    assertEquals("AS-REQ", m.messageType);
    assertEquals("EXAMPLE.COM", m.realm);
    assertEquals("alice", m.clientName);
    assertEquals("krbtgt/EXAMPLE.COM", m.serverName);
    assertEquals(Arrays.asList(18, 17, 23), m.encryptionTypes);
    assertEquals(Boolean.TRUE, m.preAuthPresent);
    assertEquals(Instant.parse("2024-01-02T03:04:05Z"), m.till);
    assertNull(m.ticketEncryptionType);
    assertNull(m.errorCode);
  }

  @Test
  public void testAsReqNoPreauth() {
    KerberosMessage m = parse(88, asReq(false), new Context(false));
    assertEquals(Boolean.FALSE, m.preAuthPresent);
  }

  @Test
  public void testAsRepKerberoast() {
    KerberosMessage m = parse(88, asRep(), new Context(false));
    assertEquals("AS-REP", m.messageType);
    assertEquals("EXAMPLE.COM", m.realm);
    assertEquals("alice", m.clientName);
    assertEquals("HTTP/web.example.com", m.serverName);
    assertEquals(Integer.valueOf(23), m.ticketEncryptionType);
    assertEquals(Boolean.FALSE, m.preAuthPresent);
    assertTrue(m.encryptionTypes.isEmpty());
  }

  @Test
  public void testKrbError() {
    KerberosMessage m = parse(88, krbError(), new Context(false));
    assertEquals("KRB-ERROR", m.messageType);
    assertEquals(Integer.valueOf(25), m.errorCode);
    assertEquals("KDC_ERR_PREAUTH_REQUIRED", m.errorText);
    assertEquals("EXAMPLE.COM", m.realm);
    assertEquals("krbtgt/EXAMPLE.COM", m.serverName);
  }

  @Test
  public void testTcpWithLengthPrefix() {
    byte[] msg = asReq(true);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(msg.length >> 24);
    out.write(msg.length >> 16);
    out.write(msg.length >> 8);
    out.write(msg.length);
    out.write(msg, 0, msg.length);
    PacketDecoder packet = TestPackets.tcp("10.0.0.1", 40000, "10.0.0.2", 88, 1, TestPackets.PSH, out.toByteArray());
    assertTrue(decoder.accepts(packet));
    KerberosMessage m = decoder.parse(packet, out.toByteArray(), new Context(false));
    assertEquals("AS-REQ", m.messageType);
  }

  @Test
  public void testNotKerberos() {
    assertNull(parse(88, "GET / HTTP/1.1\r\n\r\n".getBytes(StandardCharsets.UTF_8), new Context(false)));
    // A known application tag but pvno is not 5
    byte[] badPvno = app(10, seq(ctx(1, integer(4)), ctx(2, integer(10))));
    assertNull(parse(88, badPvno, new Context(false)));
    // An unknown application tag
    assertNull(parse(88, app(5, seq(ctx(0, integer(5)))), new Context(false)));
    assertNull(parse(88, new byte[] {0x6A}, new Context(false)));
  }

  @Test
  public void testMalformed() {
    byte[] m = asReq(true);
    try {
      parse(88, Arrays.copyOf(m, m.length - 4), new Context(false));
      fail("expected malformed Kerberos");
    } catch (IllegalArgumentException e) {
      // expected: a recognised message whose inner lengths overrun the captured bytes
    }
  }
}
