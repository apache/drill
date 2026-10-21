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
package org.apache.drill.exec.store.pcap.protocol.radius;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.TestPackets;
import org.apache.drill.exec.store.pcapng.PacketDecoder;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestRadiusDecoder extends BaseTest {

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

  private final RadiusDecoder decoder = new RadiusDecoder();

  static byte[] attr(int type, byte[] value) {
    byte[] out = new byte[2 + value.length];
    out[0] = (byte) type;
    out[1] = (byte) out.length;
    System.arraycopy(value, 0, out, 2, value.length);
    return out;
  }

  static byte[] attr(int type, String value) {
    return attr(type, value.getBytes());
  }

  static byte[] u32(long v) {
    return ByteBuffer.allocate(4).putInt((int) v).array();
  }

  static byte[] message(int code, int id, byte[]... attributes) {
    ByteArrayOutputStream body = new ByteArrayOutputStream();
    for (byte[] a : attributes) {
      body.write(a, 0, a.length);
    }
    byte[] attrs = body.toByteArray();
    ByteBuffer b = ByteBuffer.allocate(20 + attrs.length);
    b.put((byte) code).put((byte) id).putShort((short) (20 + attrs.length));
    for (int i = 0; i < 16; i++) {
      b.put((byte) i);
    }
    return b.put(attrs).array();
  }

  private RadiusMessage parse(int port, byte[] payload, DecoderContext context) {
    PacketDecoder packet = TestPackets.udp("10.0.0.1", 40000, "10.0.0.2", port, payload);
    assertTrue(decoder.accepts(packet));
    return decoder.parse(packet, payload, context);
  }

  @Test
  public void testAccessRequest() {
    byte[] m = message(1, 7, attr(1, "alice"), attr(2, new byte[16]), attr(4, new byte[] {10, 0, 0, 2}),
        attr(5, u32(3)), attr(32, "nas01"), attr(31, "00-11-22-33-44-55"), attr(30, "AP-1"));
    RadiusMessage r = parse(1812, m, new Context(false));
    assertEquals(1, r.code);
    assertEquals("Access-Request", r.codeName);
    assertEquals(7, r.identifier);
    assertEquals("000102030405060708090a0b0c0d0e0f", r.authenticator);
    assertEquals("alice", r.username);
    assertTrue(r.passwordPresent);
    assertEquals("10.0.0.2", r.nasIpAddress);
    assertEquals(Long.valueOf(3), r.nasPort);
    assertEquals("nas01", r.nasIdentifier);
    assertEquals("00-11-22-33-44-55", r.callingStationId);
    assertEquals("AP-1", r.calledStationId);
    assertEquals(7, r.attributes.size());
    assertEquals(2, r.attributes.get(1).type);
    // The encrypted password is withheld unless credentials are exposed
    assertNull(r.attributes.get(1).value);
    assertEquals("616c696365", r.attributes.get(0).value);
    assertEquals("00000000000000000000000000000000", parse(1812, m, new Context(true)).attributes.get(1).value);
  }

  @Test
  public void testAccountingAndAccept() {
    RadiusMessage acct = parse(1646, message(4, 9, attr(40, u32(1)), attr(44, "S-1"), attr(8, new byte[] {10, 1, 2, 3})),
        new Context(false));
    assertEquals("Accounting-Request", acct.codeName);
    assertEquals("Start", acct.acctStatusType);
    assertEquals("S-1", acct.acctSessionId);
    assertEquals("10.1.2.3", acct.framedIpAddress);
    assertFalse(acct.passwordPresent);
    RadiusMessage accept = parse(1645, message(2, 7, attr(18, "Welcome")), new Context(false));
    assertEquals("Access-Accept", accept.codeName);
    assertEquals("Welcome", accept.replyMessage);
    assertNull(accept.username);
  }

  @Test
  public void testNotRadius() {
    assertNull(parse(1812, "GET / HTTP/1.1\r\nHost: example\r\n\r\n".getBytes(), new Context(false)));
    assertNull(parse(1812, new byte[10], new Context(false)));
    // Unknown code
    assertNull(parse(1812, message(99, 1, attr(1, "x")), new Context(false)));
    // Length does not match: extra bytes after the message
    byte[] m = message(1, 1, attr(1, "x"));
    assertNull(parse(1812, Arrays.copyOf(m, m.length + 5), new Context(false)));
  }

  @Test
  public void testMalformed() {
    byte[] m = message(1, 1, attr(1, "alice"), attr(4, new byte[] {10, 0, 0, 2}));
    try {
      parse(1812, Arrays.copyOf(m, m.length - 2), new Context(false));
      fail("expected truncated RADIUS");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("truncated"));
    }
    byte[] bad = message(1, 1, attr(1, "alice"), new byte[] {4, 1});
    try {
      parse(1812, bad, new Context(false));
      fail("expected malformed RADIUS");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("attribute 2"));
    }
  }

  @Test
  public void testAttributeCap() {
    byte[][] attrs = new byte[70][];
    for (int i = 0; i < 70; i++) {
      attrs[i] = attr(26, new byte[] {0, 0, 0, 9, 1, 3, 'x'});
    }
    Context context = new Context(false);
    RadiusMessage r = parse(1813, message(4, 1, attrs), context);
    assertEquals(64, r.attributes.size());
    assertEquals(1, context.warnings.size());
  }
}
