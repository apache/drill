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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.protocol.TestPackets;
import org.apache.drill.exec.store.pcapng.PacketDecoder;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestPacketDecodeErrors extends BaseTest {

  @Test
  public void testMalformedIpv4HeaderInPcapng() {
    byte[] raw = TestPackets.ipv4("10.0.0.1", "10.0.0.2", 17, new byte[8]);
    raw[0] = 0x43; // IHL 3: shorter than the minimum header
    PacketDecoder packet = new PacketDecoder();
    // Still an IPv4 packet: the addresses are read, the transport layer is not
    assertTrue(packet.readPcapng(raw, 101));
    assertNotNull(packet.getDecodeError());
    assertTrue(packet.getDecodeError(), packet.getDecodeError().contains("header length"));
    assertEquals("10.0.0.1", packet.getSourceIpAddressString());
    assertFalse(packet.isUdpPacket());
    assertEquals(0, packet.getSrc_port());
  }

  @Test
  public void testMalformedIpv4HeaderInPcap() {
    // PCAP record header (16 bytes, little endian) followed by an Ethernet frame with IHL 3
    byte[] ip = TestPackets.ipv4("10.0.0.1", "10.0.0.2", 17, new byte[8]);
    ip[0] = 0x43;
    byte[] record = new byte[16 + 14 + ip.length];
    ByteBuffer.wrap(record).order(ByteOrder.LITTLE_ENDIAN)
        .putInt(0).putInt(0).putInt(14 + ip.length).putInt(14 + ip.length);
    record[16 + 12] = 0x08; // EtherType IPv4
    System.arraycopy(ip, 0, record, 16 + 14, ip.length);
    Packet packet = new Packet();
    packet.decodePcap(record, 0, false, 65535);
    assertTrue(packet.isCorrupt());
    assertNotNull(packet.getDecodeError());
    assertEquals("10.0.0.2", packet.getDestinationIpAddressString());
    assertEquals(0, packet.getDst_port());
  }

  @Test
  public void testValidPacketHasNoError() {
    PacketDecoder packet = TestPackets.udp("10.0.0.1", 1000, "10.0.0.2", 53, new byte[] {1, 2});
    assertNull(packet.getDecodeError());
    assertEquals(53, packet.getDst_port());
  }
}
