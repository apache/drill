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
package org.apache.drill.exec.store.pcap.protocol.snmp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.TestPackets;
import org.apache.drill.exec.store.pcapng.PacketDecoder;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestSnmpDecoder extends BaseTest {

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

  private final SnmpDecoder decoder = new SnmpDecoder();

  static byte[] tlv(int tag, byte[]... parts) {
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

  static byte[] integer(long v) {
    byte[] b = java.math.BigInteger.valueOf(v).toByteArray();
    return tlv(0x02, b);
  }

  static byte[] octets(String s) {
    return tlv(0x04, s.getBytes());
  }

  /** OID 1.3.6.1.2.1.1.(sub).0 */
  static byte[] oid(int sub) {
    return tlv(0x06, new byte[] {0x2B, 6, 1, 2, 1, 1, (byte) sub, 0});
  }

  static byte[] varbind(byte[] name, byte[] value) {
    return tlv(0x30, name, value);
  }

  static byte[] pdu(int tag, long requestId, byte[]... varbinds) {
    return tlv(tag, integer(requestId), integer(0), integer(0), tlv(0x30, varbinds));
  }

  static byte[] v2c(String community, byte[] pdu) {
    return tlv(0x30, integer(1), octets(community), pdu);
  }

  private SnmpMessage parse(int port, byte[] payload, DecoderContext context) {
    PacketDecoder packet = TestPackets.udp("10.0.0.1", 40000, "10.0.0.2", port, payload);
    assertTrue(decoder.accepts(packet));
    return decoder.parse(packet, payload, context);
  }

  @Test
  public void testGetRequest() {
    byte[] m = v2c("public", pdu(0xA0, 1234, varbind(oid(1), tlv(0x05)), varbind(oid(5), tlv(0x05))));
    SnmpMessage s = parse(161, m, new Context(false));
    assertEquals("v2c", s.version);
    assertTrue(s.communityPresent);
    assertNull(s.community);
    assertEquals("get-request", s.pduType);
    assertEquals(Long.valueOf(1234), s.requestId);
    assertEquals(Integer.valueOf(0), s.errorStatus);
    assertEquals(2, s.varbinds.size());
    assertEquals("1.3.6.1.2.1.1.1.0", s.varbinds.get(0).oid);
    assertEquals("null", s.varbinds.get(0).valueType);
    assertNull(s.varbinds.get(0).value);
    assertEquals("public", parse(161, m, new Context(true)).community);
  }

  @Test
  public void testResponseValues() {
    byte[] m = v2c("public", pdu(0xA2, -5,
        varbind(oid(1), octets("Linux box")),
        varbind(oid(3), tlv(0x43, new byte[] {0x01, 0x00})),
        varbind(oid(2), tlv(0x06, new byte[] {0x2B, 6, 1, 4, 1, (byte) 0x82, 0x37})),
        varbind(oid(4), tlv(0x40, new byte[] {10, 0, 0, 1})),
        varbind(oid(6), tlv(0x46, new byte[] {0, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
            (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF})),
        varbind(oid(7), integer(-3)),
        varbind(oid(8), tlv(0x04, new byte[] {0, 1, (byte) 0xFE})),
        varbind(oid(9), tlv(0x81))));
    SnmpMessage s = parse(161, m, new Context(false));
    assertEquals("get-response", s.pduType);
    assertEquals(Long.valueOf(-5), s.requestId);
    assertEquals("Linux box", s.varbinds.get(0).value);
    assertEquals("octet_string", s.varbinds.get(0).valueType);
    assertEquals("timeticks", s.varbinds.get(1).valueType);
    assertEquals("256", s.varbinds.get(1).value);
    assertEquals("1.3.6.1.4.1.311", s.varbinds.get(2).value);
    assertEquals("10.0.0.1", s.varbinds.get(3).value);
    assertEquals("18446744073709551615", s.varbinds.get(4).value);
    assertEquals("-3", s.varbinds.get(5).value);
    assertEquals("0001fe", s.varbinds.get(6).value);
    assertEquals("no_such_instance", s.varbinds.get(7).valueType);
  }

  @Test
  public void testV1Trap() {
    byte[] trap = tlv(0xA4, tlv(0x06, new byte[] {0x2B, 6, 1, 4, 1, 9}), tlv(0x40, new byte[] {10, 0, 0, 9}),
        integer(6), integer(42), tlv(0x43, new byte[] {0x10}), tlv(0x30, varbind(oid(1), octets("x"))));
    SnmpMessage s = parse(162, tlv(0x30, integer(0), octets("traps"), trap), new Context(false));
    assertEquals("v1", s.version);
    assertEquals("trap", s.pduType);
    assertEquals("1.3.6.1.4.1.9", s.enterprise);
    assertEquals("10.0.0.9", s.agentAddress);
    assertEquals(Integer.valueOf(6), s.genericTrap);
    assertEquals(Long.valueOf(42), s.specificTrap);
    assertEquals(Long.valueOf(16), s.timeStamp);
    assertNull(s.requestId);
    assertEquals(1, s.varbinds.size());
  }

  @Test
  public void testGetBulk() {
    byte[] bulk = tlv(0xA5, integer(9), integer(1), integer(10), tlv(0x30, varbind(oid(1), tlv(0x05))));
    SnmpMessage s = parse(161, v2c("public", bulk), new Context(false));
    assertEquals("get-bulk-request", s.pduType);
    assertEquals(Integer.valueOf(1), s.nonRepeaters);
    assertEquals(Integer.valueOf(10), s.maxRepetitions);
    assertNull(s.errorStatus);
  }

  private static byte[] v3(int flags, String user, byte[] data) {
    byte[] global = tlv(0x30, integer(77), integer(65507), tlv(0x04, new byte[] {(byte) flags}), integer(3));
    byte[] usm = tlv(0x30, tlv(0x04, new byte[] {(byte) 0x80, 0, 0x1F, (byte) 0x88}), integer(1), integer(100),
        octets(user), tlv(0x04, new byte[12]), tlv(0x04, new byte[8]));
    return tlv(0x30, integer(3), global, tlv(0x04, usm), data);
  }

  @Test
  public void testV3() {
    byte[] scoped = tlv(0x30, tlv(0x04), tlv(0x04), pdu(0xA0, 5, varbind(oid(1), tlv(0x05))));
    SnmpMessage s = parse(161, v3(0x04, "bob", scoped), new Context(true));
    assertEquals("v3", s.version);
    assertEquals("bob", s.msgUserName);
    assertEquals("noAuthNoPriv", s.securityLevel);
    assertEquals(Long.valueOf(77), s.msgId);
    assertEquals("80001f88", s.engineId);
    assertFalse(s.communityPresent);
    assertFalse(s.encrypted);
    assertEquals("get-request", s.pduType);
    assertEquals(1, s.varbinds.size());

    SnmpMessage enc = parse(161, v3(0x07, "carol", tlv(0x04, new byte[24])), new Context(false));
    assertEquals("authPriv", enc.securityLevel);
    assertTrue(enc.encrypted);
    assertNull(enc.pduType);
    assertEquals(0, enc.varbinds.size());
  }

  @Test
  public void testNotSnmp() {
    assertNull(parse(161, "GET / HTTP/1.1\r\n\r\n".getBytes(), new Context(false)));
    assertNull(parse(161, new byte[] {0x30, 0x03, 0x02, 0x01, 0x07}, new Context(false)));
    assertNull(parse(161, new byte[] {0x30}, new Context(false)));
    // Indefinite length
    assertNull(parse(161, new byte[] {0x30, (byte) 0x80, 0x02, 0x01, 0x01, 0, 0}, new Context(false)));
  }

  @Test
  public void testMalformed() {
    byte[] m = v2c("public", pdu(0xA0, 1, varbind(oid(1), tlv(0x05))));
    try {
      parse(161, Arrays.copyOf(m, m.length - 3), new Context(false));
      fail("expected truncated SNMP");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("truncated"));
    }
    byte[] badPdu = v2c("public", tlv(0xAF, integer(1)));
    try {
      parse(161, badPdu, new Context(false));
      fail("expected malformed SNMP");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("PDU"));
    }
    // A varbind length that overruns its list
    byte[] overrun = v2c("public", tlv(0xA0, integer(1), integer(0), integer(0),
        new byte[] {0x30, 0x04, 0x30, 0x10, 0x06, 0x00}));
    try {
      parse(161, overrun, new Context(false));
      fail("expected malformed SNMP");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("length"));
    }
  }

  @Test
  public void testVarbindCap() {
    byte[][] varbinds = new byte[70][];
    for (int i = 0; i < 70; i++) {
      varbinds[i] = varbind(oid(1), integer(i));
    }
    Context context = new Context(false);
    SnmpMessage s = parse(161, v2c("public", pdu(0xA2, 1, varbinds)), context);
    assertEquals(64, s.varbinds.size());
    assertEquals(1, context.warnings.size());
  }
}
