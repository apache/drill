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
package org.apache.drill.exec.store.pcap.protocol.stun;

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

public class TestStunDecoder extends BaseTest {

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

  private static final byte[] TXID = hex("b7e7a701bc34d686fa87dfae");

  private final StunDecoder decoder = new StunDecoder();

  private static byte[] hex(String s) {
    byte[] b = new byte[s.length() / 2];
    for (int i = 0; i < b.length; i++) {
      b[i] = (byte) Integer.parseInt(s.substring(2 * i, 2 * i + 2), 16);
    }
    return b;
  }

  private static byte[] attr(int type, byte[] value) {
    int padded = (value.length + 3) & ~3;
    return ByteBuffer.allocate(4 + padded).putShort((short) type).putShort((short) value.length).put(value).array();
  }

  private static byte[] text(int type, String value) {
    return attr(type, value.getBytes(StandardCharsets.UTF_8));
  }

  private static byte[] message(int type, byte[]... attributes) {
    ByteArrayOutputStream body = new ByteArrayOutputStream();
    for (byte[] a : attributes) {
      body.write(a, 0, a.length);
    }
    return ByteBuffer.allocate(20 + body.size()).putShort((short) type).putShort((short) body.size())
        .putInt(0x2112A442).put(TXID).put(body.toByteArray()).array();
  }

  private StunMessage parse(byte[] payload, Context context) {
    Packet packet = TestPackets.udp("10.0.0.1", 50000, "10.0.0.2", 3478, payload);
    return decoder.parse(packet, payload, context);
  }

  @Test
  public void testAcceptsStunPortsOverUdpOnly() {
    assertTrue(decoder.accepts(TestPackets.udp("10.0.0.1", 50000, "10.0.0.2", 3478, new byte[1])));
    assertTrue(decoder.accepts(TestPackets.udp("10.0.0.2", 19302, "10.0.0.1", 50000, new byte[1])));
    assertFalse(decoder.accepts(TestPackets.udp("10.0.0.1", 50000, "10.0.0.2", 53, new byte[1])));
    assertFalse(decoder.accepts(TestPackets.tcp("10.0.0.1", 50000, "10.0.0.2", 3478, 1, TestPackets.ACK, new byte[1])));
  }

  @Test
  public void testRfc5769Ipv4Response() {
    // RFC 5769 section 2.2, the sample IPv4 response
    byte[] payload = hex("0101003c2112a442b7e7a701bc34d686fa87dfae"
        + "8022000b7465737420766563746f7220"
        + "002000080001a147e112a643"
        + "000800142b91f599fd9e90c38c7489f92af9ba53f06be7d7"
        + "80280004c07d4c96");
    Context context = new Context();
    StunMessage m = parse(payload, context);
    assertEquals("success_response", m.messageClass);
    assertEquals("binding", m.messageMethod);
    assertEquals("b7e7a701bc34d686fa87dfae", m.transactionId);
    assertEquals("test vector", m.software);  // The trailing 0x20 is padding
    assertEquals("192.0.2.1:32853", m.xorMappedAddress);
    assertEquals(4, m.attributes.size());
    assertEquals(0x8022, m.attributes.get(0)[0]);
    assertEquals(11, m.attributes.get(0)[1]);
    assertEquals(0x8028, m.attributes.get(3)[0]);
    assertNull(m.username);
    assertTrue(context.warnings.isEmpty());
  }

  @Test
  public void testRfc5769Ipv6Response() {
    // RFC 5769 section 2.3, the sample IPv6 response (XOR-MAPPED-ADDRESS only)
    byte[] payload = message(0x0101, attr(0x0020, hex("0002a1470113a9faa5d3f179bc25f4b5bed2b9d9")));
    StunMessage m = parse(payload, new Context());
    assertEquals("[2001:db8:1234:5678:11:2233:4455:6677]:32853", m.xorMappedAddress);
  }

  @Test
  public void testRequestWithUsername() {
    StunMessage m = parse(message(0x0001, text(0x0006, "evtj:h6vY"), text(0x8022, "client 1.0")), new Context());
    assertEquals("request", m.messageClass);
    assertEquals("binding", m.messageMethod);
    assertEquals("evtj:h6vY", m.username);
    assertEquals("client 1.0", m.software);
    assertEquals(2, m.attributes.size());
  }

  @Test
  public void testErrorResponse() {
    byte[] error = ByteBuffer.allocate(4 + 12).putShort((short) 0).put((byte) 4).put((byte) 1)
        .put("Unauthorized".getBytes(StandardCharsets.US_ASCII)).array();
    StunMessage m = parse(message(0x0113, attr(0x0009, error), text(0x0014, "example.org"),
        text(0x0015, "f//499k954d6OL34oL9FSTvy64sA")), new Context());
    assertEquals("error_response", m.messageClass);
    assertEquals("allocate", m.messageMethod);
    assertEquals(Integer.valueOf(401), m.errorCode);
    assertEquals("Unauthorized", m.errorReason);
    assertEquals("example.org", m.realm);
    assertEquals("f//499k954d6OL34oL9FSTvy64sA", m.nonce);
  }

  @Test
  public void testMethodsAndClasses() {
    assertEquals("indication", parse(message(0x0016), new Context()).messageClass);
    assertEquals("send", parse(message(0x0016), new Context()).messageMethod);
    assertEquals("channel_bind", parse(message(0x0009), new Context()).messageMethod);
    assertEquals("create_permission", parse(message(0x0108), new Context()).messageMethod);
    assertEquals("refresh", parse(message(0x0004), new Context()).messageMethod);
    assertEquals("data", parse(message(0x0017), new Context()).messageMethod);
    // Method 0x00A is not named; method bits above the class bits: 0x0201 is method 0x81
    assertEquals("10", parse(message(0x000A), new Context()).messageMethod);
    assertEquals("129", parse(message(0x0201), new Context()).messageMethod);
  }

  @Test
  public void testMappedAddress() {
    StunMessage m = parse(message(0x0101, attr(0x0001, hex("00011f90c0000201"))), new Context());
    assertEquals("192.0.2.1:8080", m.mappedAddress);
  }

  @Test
  public void testNotStun() {
    Context context = new Context();
    byte[] good = message(0x0001, text(0x8022, "x"));
    assertNull(parse(new byte[19], context));
    byte[] noCookie = good.clone();
    noCookie[4] = 0;
    assertNull(parse(noCookie, context));
    byte[] topBits = good.clone();
    topBits[0] = (byte) 0x80;
    assertNull(parse(topBits, context));
    byte[] oddLength = good.clone();
    oddLength[3] = 7;
    assertNull(parse(oddLength, context));
    byte[] tooLong = good.clone();
    tooLong[3] = 64;
    assertNull(parse(tooLong, context));
    assertTrue(context.warnings.isEmpty());
  }

  @Test
  public void testAttributeOverrunThrows() {
    byte[] payload = message(0x0001, text(0x8022, "abcd"));
    payload[23] = 40;
    try {
      parse(payload, new Context());
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().startsWith("attribute 0x8022 length 40"));
    }
  }

  @Test
  public void testBadAddressThrows() {
    try {
      parse(message(0x0101, attr(0x0020, hex("0003a147e112a643"))), new Context());
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("XOR-MAPPED-ADDRESS"));
    }
  }

  @Test
  public void testAttributesAreCapped() {
    Context context = new Context();
    byte[][] attributes = new byte[70][];
    Arrays.fill(attributes, attr(0x8023, new byte[0]));
    StunMessage m = parse(message(0x0001, attributes), context);
    assertEquals(64, m.attributes.size());
    assertEquals(Collections.singletonList("attributes truncated to 64"), context.warnings);
  }
}
