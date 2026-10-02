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
package org.apache.drill.exec.store.pcap.protocol;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;

import org.apache.drill.exec.store.pcapng.PacketDecoder;

/** Builds decoded raw-IP packets for unit tests. */
public final class TestPackets {
  public static final int FIN = 0x01;
  public static final int SYN = 0x02;
  public static final int RST = 0x04;
  public static final int PSH = 0x08;
  public static final int ACK = 0x10;
  private static final int LINKTYPE_RAW = 101;

  private TestPackets() { }

  public static PacketDecoder udp(String src, int srcPort, String dst, int dstPort, byte[] payload) {
    ByteBuffer udp = ByteBuffer.allocate(8 + payload.length);
    udp.putShort((short) srcPort).putShort((short) dstPort).putShort((short) (8 + payload.length)).putShort((short) 0).put(payload);
    return decode(ipv4(src, dst, 17, udp.array()), Instant.EPOCH);
  }

  public static PacketDecoder tcp(String src, int srcPort, String dst, int dstPort, long seq, int flags, byte[] payload) {
    return tcp(src, srcPort, dst, dstPort, seq, flags, payload, Instant.EPOCH);
  }

  public static PacketDecoder tcp(String src, int srcPort, String dst, int dstPort, long seq, int flags, byte[] payload,
                                  Instant timestamp) {
    ByteBuffer tcp = ByteBuffer.allocate(20 + payload.length);
    tcp.putShort((short) srcPort).putShort((short) dstPort).putInt((int) seq).putInt(0)
        .put((byte) (5 << 4)).put((byte) flags).putShort((short) 65535).putShort((short) 0).putShort((short) 0)
        .put(payload);
    return decode(ipv4(src, dst, 6, tcp.array()), timestamp);
  }

  public static byte[] ipv4(String src, String dst, int protocol, byte[] transport) {
    ByteBuffer ip = ByteBuffer.allocate(20 + transport.length);
    ip.put((byte) 0x45).put((byte) 0).putShort((short) (20 + transport.length)).putShort((short) 0).putShort((short) 0)
        .put((byte) 64).put((byte) protocol).putShort((short) 0).put(address(src)).put(address(dst)).put(transport);
    return ip.array();
  }

  public static PacketDecoder decode(byte[] rawIp, Instant timestamp) {
    PacketDecoder packet = new PacketDecoder();
    packet.readPcapng(rawIp, LINKTYPE_RAW);
    packet.setTimestamp(timestamp);
    return packet;
  }

  private static byte[] address(String host) {
    try {
      return InetAddress.getByName(host).getAddress();
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
