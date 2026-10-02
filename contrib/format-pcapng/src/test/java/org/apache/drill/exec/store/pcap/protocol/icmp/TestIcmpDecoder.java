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
package org.apache.drill.exec.store.pcap.protocol.icmp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;

import org.apache.drill.exec.store.pcap.protocol.TestPackets;
import org.apache.drill.exec.store.pcapng.PacketDecoder;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestIcmpDecoder extends BaseTest {
  private final IcmpDecoder decoder = new IcmpDecoder();

  private static byte[] ip(String host) throws Exception {
    return InetAddress.getByName(host).getAddress();
  }

  private static byte[] concat(byte[]... parts) {
    int n = 0;
    for (byte[] p : parts) {
      n += p.length;
    }
    ByteBuffer out = ByteBuffer.allocate(n);
    for (byte[] p : parts) {
      out.put(p);
    }
    return out.array();
  }

  /** An IPv6 header with the given next header, followed by the message. */
  private static byte[] ipv6(String src, String dst, int nextHeader, byte[] payload) throws Exception {
    return ByteBuffer.allocate(40 + payload.length).putInt(0x60000000).putShort((short) payload.length)
        .put((byte) nextHeader).put((byte) 64).put(ip(src)).put(ip(dst)).put(payload).array();
  }

  private static PacketDecoder rawIp(byte[] packet) {
    return TestPackets.decode(packet, Instant.EPOCH);
  }

  @Test
  public void testEchoRequestV4() {
    byte[] icmp = {8, 0, 0, 0, 0x12, 0x34, 0, 7, 'h', 'i'};
    PacketDecoder packet = rawIp(TestPackets.ipv4("10.0.0.1", "10.0.0.2", 1, icmp));
    assertTrue(decoder.accepts(packet));
    IcmpMessage m = decoder.parse(packet, null, null);
    assertEquals(4, m.version);
    assertEquals(8, m.type);
    assertEquals(0, m.code);
    assertEquals("echo_request", m.typeName);
    assertNull(m.codeName);
    assertEquals(Integer.valueOf(0x1234), m.identifier);
    assertEquals(Integer.valueOf(7), m.sequence);
    assertNull(m.originalSrcIp);
  }

  @Test
  public void testNotAcceptedForUdp() {
    assertFalse(decoder.accepts(TestPackets.udp("10.0.0.1", 1, "10.0.0.2", 2, new byte[8])));
  }

  @Test
  public void testPortUnreachableWithEmbeddedUdp() throws Exception {
    byte[] udp = ByteBuffer.allocate(8).putShort((short) 5000).putShort((short) 53).putShort((short) 8).array();
    byte[] original = TestPackets.ipv4("10.0.0.1", "8.8.8.8", 17, udp);
    byte[] icmp = concat(new byte[] {3, 3, 0, 0, 0, 0, 0, 0}, original);
    IcmpMessage m = IcmpDecoder.parse(icmp, 4);
    assertEquals("destination_unreachable", m.typeName);
    assertEquals("port_unreachable", m.codeName);
    assertEquals("10.0.0.1", m.originalSrcIp);
    assertEquals("8.8.8.8", m.originalDstIp);
    assertEquals(Integer.valueOf(17), m.originalProtocol);
    assertEquals(Integer.valueOf(5000), m.originalSrcPort);
    assertEquals(Integer.valueOf(53), m.originalDstPort);
    assertNull(m.mtu);
  }

  @Test
  public void testFragmentationNeededAndShortEmbeddedPacket() throws Exception {
    // Embedded IPv4 header only, transport header not captured
    byte[] original = TestPackets.ipv4("10.0.0.1", "10.0.0.9", 6, new byte[2]);
    byte[] icmp = concat(new byte[] {3, 4, 0, 0, 0, 0, 0x05, (byte) 0xDC}, original);
    IcmpMessage m = IcmpDecoder.parse(icmp, 4);
    assertEquals("fragmentation_needed", m.codeName);
    assertEquals(Integer.valueOf(1500), m.mtu);
    assertEquals(Integer.valueOf(6), m.originalProtocol);
    assertNull(m.originalSrcPort);
    assertNull(m.originalDstPort);
  }

  @Test
  public void testEmbeddedHeaderCutShortIsIgnored() {
    byte[] icmp = {11, 0, 0, 0, 0, 0, 0, 0, 0x45, 0, 0, 20};
    IcmpMessage m = IcmpDecoder.parse(icmp, 4);
    assertEquals("time_exceeded", m.typeName);
    assertEquals("ttl_exceeded_in_transit", m.codeName);
    assertNull(m.originalSrcIp);
    assertNull(m.originalProtocol);
  }

  @Test
  public void testRedirectGateway() throws Exception {
    byte[] icmp = concat(new byte[] {5, 1, 0, 0}, ip("192.168.1.254"));
    IcmpMessage m = IcmpDecoder.parse(icmp, 4);
    assertEquals("redirect", m.typeName);
    assertEquals("redirect_for_host", m.codeName);
    assertEquals("192.168.1.254", m.gateway);
  }

  @Test
  public void testUnknownTypeIsStillIcmp() {
    IcmpMessage m = IcmpDecoder.parse(new byte[] {(byte) 200, 1, 0, 0}, 4);
    assertEquals(200, m.type);
    assertEquals(1, m.code);
    assertNull(m.typeName);
  }

  @Test
  public void testTooShortIsNotIcmp() {
    assertNull(IcmpDecoder.parse(new byte[] {8, 0, 0}, 4));
    assertNull(IcmpDecoder.parse(null, 4));
    byte[] raw = TestPackets.ipv4("10.0.0.1", "10.0.0.2", 1, new byte[0]);
    assertNull(decoder.parse(rawIp(raw), null, null));
  }

  @Test
  public void testTruncatedEchoIsMalformed() {
    try {
      IcmpDecoder.parse(new byte[] {8, 0, 0, 0, 0, 1}, 4);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("truncated echo_request: 6 bytes, needs 8", e.getMessage());
    }
  }

  @Test
  public void testEchoReplyV6() throws Exception {
    byte[] icmp = {(byte) 129, 0, 0, 0, 0, 5, 0, 9};
    PacketDecoder packet = rawIp(ipv6("2001:db8::1", "2001:db8::2", 58, icmp));
    assertTrue(decoder.accepts(packet));
    IcmpMessage m = decoder.parse(packet, null, null);
    assertEquals(6, m.version);
    assertEquals("echo_reply", m.typeName);
    assertEquals(Integer.valueOf(5), m.identifier);
    assertEquals(Integer.valueOf(9), m.sequence);
  }

  @Test
  public void testNeighborAdvertisement() throws Exception {
    byte[] icmp = concat(new byte[] {(byte) 136, 0, 0, 0, 0x60, 0, 0, 0}, ip("fe80::1"));
    IcmpMessage m = IcmpDecoder.parse(icmp, 6);
    assertEquals("neighbor_advertisement", m.typeName);
    assertEquals("fe80:0:0:0:0:0:0:1", m.targetAddress);
  }

  @Test
  public void testRedirectV6() throws Exception {
    byte[] icmp = concat(new byte[] {(byte) 137, 0, 0, 0, 0, 0, 0, 0}, ip("fe80::1"), ip("2001:db8::5"));
    IcmpMessage m = IcmpDecoder.parse(icmp, 6);
    assertEquals("redirect", m.typeName);
    assertEquals("fe80:0:0:0:0:0:0:1", m.targetAddress);
    assertEquals("2001:db8:0:0:0:0:0:5", m.destinationAddress);
  }

  @Test
  public void testTruncatedNeighborSolicitationIsMalformed() {
    try {
      IcmpDecoder.parse(new byte[] {(byte) 135, 0, 0, 0, 0, 0, 0, 0, 1, 2}, 6);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("truncated neighbor_solicitation: 10 bytes, needs 24", e.getMessage());
    }
  }

  @Test
  public void testPacketTooBigWithEmbeddedTcpAfterExtensionHeader() throws Exception {
    // Hop-by-hop options header (8 bytes) then TCP
    byte[] hopByHop = {6, 0, 0, 0, 0, 0, 0, 0};
    byte[] tcp = ByteBuffer.allocate(4).putShort((short) 443).putShort((short) 50000).array();
    byte[] original = ipv6("2001:db8::1", "2001:db8::2", 0, concat(hopByHop, tcp));
    byte[] icmp = concat(new byte[] {2, 0, 0, 0, 0, 0, 0x05, 0x00}, original);
    IcmpMessage m = IcmpDecoder.parse(icmp, 6);
    assertEquals("packet_too_big", m.typeName);
    assertEquals(Integer.valueOf(1280), m.mtu);
    assertEquals("2001:db8:0:0:0:0:0:1", m.originalSrcIp);
    assertEquals("2001:db8:0:0:0:0:0:2", m.originalDstIp);
    assertEquals(Integer.valueOf(6), m.originalProtocol);
    assertEquals(Integer.valueOf(443), m.originalSrcPort);
    assertEquals(Integer.valueOf(50000), m.originalDstPort);
  }

  @Test
  public void testUnreachableV6CodeNames() {
    IcmpMessage m = IcmpDecoder.parse(new byte[] {1, 4, 0, 0, 0, 0, 0, 0}, 6);
    assertEquals("destination_unreachable", m.typeName);
    assertEquals("port_unreachable", m.codeName);
    assertNull(m.originalSrcIp);
  }

  @Test
  public void testExtensionHeaderLoopIsBounded() throws Exception {
    // Every extension header claims another; the walk stops at the captured bytes
    byte[] chain = new byte[64];
    for (int i = 0; i < chain.length; i += 8) {
      chain[i] = 0;
    }
    byte[] original = ipv6("2001:db8::1", "2001:db8::2", 0, chain);
    byte[] icmp = concat(new byte[] {3, 0, 0, 0, 0, 0, 0, 0}, original);
    IcmpMessage m = IcmpDecoder.parse(icmp, 6);
    assertEquals("2001:db8:0:0:0:0:0:1", m.originalSrcIp);
    assertNull(m.originalSrcPort);
  }
}
