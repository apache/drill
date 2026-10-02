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
package org.apache.drill.exec.store.pcap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;

import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.protocol.TestPackets;
import org.apache.drill.exec.store.pcapng.PacketDecoder;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestPacketPayloads extends BaseTest {
  // An ARP request: Ethernet/IPv4, who has 10.0.0.2, tell 10.0.0.1
  private static final byte[] ARP = {0, 1, 8, 0, 6, 4, 0, 1, 2, 0, 0, 0, 0, 1, 10, 0, 0, 1,
      0, 0, 0, 0, 0, 0, 10, 0, 0, 2};

  private static byte[] concat(byte[]... parts) {
    ByteBuffer out = ByteBuffer.allocate(java.util.Arrays.stream(parts).mapToInt(p -> p.length).sum());
    for (byte[] p : parts) {
      out.put(p);
    }
    return out.array();
  }

  @Test
  public void testIcmpMessageIsTheIpPayload() {
    byte[] icmp = {8, 0, 0, 0, 0, 1, 0, 1, 'h', 'i'};
    PacketDecoder packet = TestPackets.decode(TestPackets.ipv4("10.0.0.1", "10.0.0.2", 1, icmp), Instant.EPOCH);
    assertTrue(packet.isIcmpPacket());
    assertArrayEquals(icmp, packet.getIpPayload());
  }

  @Test
  public void testArpOverEthernet() {
    byte[] ethernet = {2, 0, 0, 0, 0, 2, 2, 0, 0, 0, 0, 1, 8, 6};
    PacketDecoder packet = new PacketDecoder();
    assertTrue(packet.readPcapng(concat(ethernet, ARP), 1));
    assertTrue(packet.isArpPacket());
    assertArrayEquals(ARP, packet.getLinkPayload());
  }

  @Test
  public void testArpOverLinuxCookedCapture() {
    // 16-byte SLL header, protocol 0x0806 at offset 14
    byte[] sll = {0, 0, 0, 1, 0, 6, 2, 0, 0, 0, 0, 1, 0, 0, 8, 6};
    PacketDecoder packet = new PacketDecoder();
    assertTrue(packet.readPcapng(concat(sll, ARP), 113));
    assertArrayEquals(ARP, packet.getLinkPayload());
  }

  @Test
  public void testNoIpPayloadWhenTheHeaderIsMalformed() {
    byte[] raw = TestPackets.ipv4("10.0.0.1", "10.0.0.2", 1, new byte[8]);
    raw[0] = 0x43;
    PacketDecoder packet = new PacketDecoder();
    packet.readPcapng(raw, 101);
    assertNull(packet.getIpPayload());
  }

  @Test
  public void testArpInPcapRecord() {
    byte[] frame = concat(new byte[] {2, 0, 0, 0, 0, 2, 2, 0, 0, 0, 0, 1, 8, 6}, ARP);
    byte[] record = new byte[16 + frame.length];
    ByteBuffer.wrap(record).order(ByteOrder.LITTLE_ENDIAN).putInt(0).putInt(0).putInt(frame.length).putInt(frame.length);
    System.arraycopy(frame, 0, record, 16, frame.length);
    Packet packet = new Packet();
    packet.decodePcap(record, 0, false, 65535);
    assertArrayEquals(ARP, packet.getLinkPayload());
  }

  @Test
  public void testPcapPacketOwnsItsBytes() {
    // The reader reuses its buffer; packets kept for sessionization must not change with it
    byte[] frame = concat(new byte[] {2, 0, 0, 0, 0, 2, 2, 0, 0, 0, 0, 1, 8, 0},
        TestPackets.ipv4("10.0.0.1", "10.0.0.2", 17, new byte[] {0, 7, 0, 9, 0, 8, 0, 0}));
    byte[] record = new byte[16 + frame.length];
    ByteBuffer.wrap(record).order(ByteOrder.LITTLE_ENDIAN).putInt(0).putInt(0).putInt(frame.length).putInt(frame.length);
    System.arraycopy(frame, 0, record, 16, frame.length);
    Packet packet = new Packet();
    packet.decodePcap(record, 0, false, 65535);
    java.util.Arrays.fill(record, (byte) 0);
    assertEquals("10.0.0.1", packet.getSourceIpAddressString());
    assertEquals(7, packet.getSrc_port());
  }
}
