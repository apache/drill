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
package org.apache.drill.exec.store.pcap.protocol.arp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;

import org.apache.drill.exec.store.pcap.protocol.TestPackets;
import org.apache.drill.exec.store.pcapng.PacketDecoder;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestArpDecoder extends BaseTest {
  private static final byte[] ETHERNET = {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
      2, 0, 0, 0, 0, 1, 8, 6};
  private final ArpDecoder decoder = new ArpDecoder();

  private static byte[] arp(int operation, int[] senderIp, int[] targetIp, int targetLastMacByte) {
    ByteBuffer b = ByteBuffer.allocate(28).putShort((short) 1).putShort((short) 0x0800).put((byte) 6).put((byte) 4)
        .putShort((short) operation).put(new byte[] {2, 0, 0, 0, 0, 1});
    for (int v : senderIp) {
      b.put((byte) v);
    }
    b.put(new byte[] {0, 0, 0, 0, 0, (byte) targetLastMacByte});
    for (int v : targetIp) {
      b.put((byte) v);
    }
    return b.array();
  }

  private static PacketDecoder frame(byte[] arp) {
    byte[] frame = new byte[ETHERNET.length + arp.length];
    System.arraycopy(ETHERNET, 0, frame, 0, ETHERNET.length);
    System.arraycopy(arp, 0, frame, ETHERNET.length, arp.length);
    PacketDecoder packet = new PacketDecoder();
    packet.readPcapng(frame, 1);
    return packet;
  }

  @Test
  public void testRequest() {
    PacketDecoder packet = frame(arp(1, new int[] {10, 0, 0, 1}, new int[] {10, 0, 0, 2}, 0));
    assertTrue(decoder.accepts(packet));
    ArpMessage m = decoder.parse(packet, null, null);
    assertEquals(1, m.hardwareType);
    assertEquals(0x0800, m.protocolType);
    assertEquals(1, m.operation);
    assertEquals("request", m.operationName);
    assertEquals("02:00:00:00:00:01", m.senderMac);
    assertEquals("10.0.0.1", m.senderIp);
    assertEquals("00:00:00:00:00:00", m.targetMac);
    assertEquals("10.0.0.2", m.targetIp);
    assertFalse(m.isGratuitous);
    assertFalse(m.isProbe);
  }

  @Test
  public void testIgnoresEthernetPadding() {
    byte[] padded = Arrays.copyOf(arp(2, new int[] {10, 0, 0, 2}, new int[] {10, 0, 0, 1}, 0xAB), 46);
    ArpMessage m = decoder.parse(frame(padded), null, null);
    assertEquals("reply", m.operationName);
    assertEquals("00:00:00:00:00:AB", m.targetMac);
  }

  @Test
  public void testGratuitousAndProbe() {
    assertTrue(ArpDecoder.parse(arp(1, new int[] {10, 0, 0, 5}, new int[] {10, 0, 0, 5}, 0)).isGratuitous);
    ArpMessage probe = ArpDecoder.parse(arp(1, new int[] {0, 0, 0, 0}, new int[] {10, 0, 0, 5}, 0));
    assertTrue(probe.isProbe);
    assertFalse(probe.isGratuitous);
  }

  @Test
  public void testOtherLengthsRenderHex() {
    // Hardware length 2, protocol length 3, operation 8 (InARP request)
    byte[] arp = {0, 6, 0x12, 0x34, 2, 3, 0, 8, (byte) 0xAA, (byte) 0xBB, 1, 2, 3, (byte) 0xCC, (byte) 0xDD, 4, 5, 6};
    ArpMessage m = ArpDecoder.parse(arp);
    assertEquals("inarp_request", m.operationName);
    assertEquals("AA:BB", m.senderMac);
    assertEquals("010203", m.senderIp);
    assertEquals("CC:DD", m.targetMac);
    assertEquals("040506", m.targetIp);
  }

  @Test
  public void testLengthsThatDoNotFitAreNotArp() {
    // Declares 16-byte hardware addresses in a 28-byte message
    byte[] arp = arp(1, new int[] {10, 0, 0, 1}, new int[] {10, 0, 0, 2}, 0);
    arp[4] = 16;
    assertNull(ArpDecoder.parse(arp));
    assertNull(ArpDecoder.parse(new byte[] {0, 1, 8, 0, 6}));
    assertNull(ArpDecoder.parse(null));
    byte[] zero = arp.clone();
    zero[4] = 0;
    zero[5] = 0;
    assertNull(ArpDecoder.parse(zero));
  }

  @Test
  public void testTruncatedEthernetIpv4IsMalformed() {
    byte[] arp = Arrays.copyOf(arp(1, new int[] {10, 0, 0, 1}, new int[] {10, 0, 0, 2}, 0), 20);
    try {
      ArpDecoder.parse(arp);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("truncated: 20 bytes, needs 28", e.getMessage());
    }
  }

  @Test
  public void testNotAcceptedForIp() {
    assertFalse(decoder.accepts(TestPackets.decode(TestPackets.ipv4("10.0.0.1", "10.0.0.2", 1, new byte[8]),
        Instant.EPOCH)));
  }
}
