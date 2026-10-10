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

import java.util.Arrays;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.PacketProtocolDecoder;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/** ARP and RARP (EtherType 0x0806). The message is the link-layer payload. */
public class ArpDecoder implements PacketProtocolDecoder<ArpMessage> {
  private static final int HEADER = 8;
  private static final int ETHERNET = 1;
  private static final int IPV4 = 0x0800;
  private static final String[] OPERATIONS = {null, "request", "reply", "rarp_request", "rarp_reply",
      "drarp_request", "drarp_reply", "drarp_error", "inarp_request", "inarp_reply", "arp_nak"};

  @Override
  public String protocol() {
    return "arp";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("hardware_type", MinorType.INT)
        .addNullable("protocol_type", MinorType.INT)
        .addNullable("operation", MinorType.INT)
        .addNullable("operation_name", MinorType.VARCHAR)
        .addNullable("sender_mac", MinorType.VARCHAR)
        .addNullable("sender_ip", MinorType.VARCHAR)
        .addNullable("target_mac", MinorType.VARCHAR)
        .addNullable("target_ip", MinorType.VARCHAR)
        .addNullable("is_gratuitous", MinorType.BIT)
        .addNullable("is_probe", MinorType.BIT);
  }

  @Override
  public boolean accepts(Packet packet) {
    return packet.isArpPacket();
  }

  @Override
  public ArpMessage parse(Packet packet, byte[] payload, DecoderContext context) {
    // payload is the TCP or UDP payload, null here; the ARP message is the link payload
    return parse(packet.getLinkPayload());
  }

  /**
   * @param b the ARP message, possibly followed by link-layer padding
   * @return null if the address lengths in the header do not fit the message
   * @throws IllegalArgumentException if an Ethernet/IPv4 message is cut short
   */
  public static ArpMessage parse(byte[] b) {
    if (b == null || b.length < HEADER) {
      return null;
    }
    int hardwareType = u16(b, 0);
    int protocolType = u16(b, 2);
    int hardwareLength = b[4] & 0xFF;
    int protocolLength = b[5] & 0xFF;
    int needed = HEADER + 2 * (hardwareLength + protocolLength);
    if (hardwareLength == 0 || protocolLength == 0) {
      return null;
    }
    if (needed > b.length) {
      if (hardwareType == ETHERNET && protocolType == IPV4 && hardwareLength == 6 && protocolLength == 4) {
        throw new IllegalArgumentException(String.format("truncated: %d bytes, needs %d", b.length, needed));
      }
      return null;
    }
    ArpMessage m = new ArpMessage();
    m.hardwareType = hardwareType;
    m.protocolType = protocolType;
    m.operation = u16(b, 6);
    m.operationName = m.operation < OPERATIONS.length ? OPERATIONS[m.operation] : null;
    int senderHardware = HEADER;
    int senderProtocol = senderHardware + hardwareLength;
    int targetHardware = senderProtocol + protocolLength;
    int targetProtocol = targetHardware + hardwareLength;
    boolean ipv4 = protocolType == IPV4 && protocolLength == 4;
    m.senderMac = hardware(b, senderHardware, hardwareLength);
    m.senderIp = protocolAddress(b, senderProtocol, protocolLength, ipv4);
    m.targetMac = hardware(b, targetHardware, hardwareLength);
    m.targetIp = protocolAddress(b, targetProtocol, protocolLength, ipv4);
    m.isGratuitous = Arrays.equals(Arrays.copyOfRange(b, senderProtocol, senderProtocol + protocolLength),
        Arrays.copyOfRange(b, targetProtocol, targetProtocol + protocolLength));
    m.isProbe = true;
    for (int i = senderProtocol; i < senderProtocol + protocolLength; i++) {
      if (b[i] != 0) {
        m.isProbe = false;
        break;
      }
    }
    return m;
  }

  /** Colon-separated upper-case hex, like the eth_src column. */
  private static String hardware(byte[] b, int offset, int length) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      if (i > 0) {
        sb.append(':');
      }
      sb.append(String.format("%02X", b[offset + i] & 0xFF));
    }
    return sb.toString();
  }

  /** Dotted quad for IPv4, otherwise hex. */
  private static String protocolAddress(byte[] b, int offset, int length, boolean ipv4) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      if (ipv4) {
        if (i > 0) {
          sb.append('.');
        }
        sb.append(b[offset + i] & 0xFF);
      } else {
        sb.append(String.format("%02x", b[offset + i] & 0xFF));
      }
    }
    return sb.toString();
  }

  private static int u16(byte[] b, int offset) {
    return ((b[offset] & 0xFF) << 8) | (b[offset + 1] & 0xFF);
  }

  @Override
  public void write(ArpMessage m, TupleWriter fields) {
    fields.scalar("hardware_type").setInt(m.hardwareType);
    fields.scalar("protocol_type").setInt(m.protocolType);
    fields.scalar("operation").setInt(m.operation);
    if (m.operationName != null) {
      fields.scalar("operation_name").setString(m.operationName);
    }
    fields.scalar("sender_mac").setString(m.senderMac);
    fields.scalar("sender_ip").setString(m.senderIp);
    fields.scalar("target_mac").setString(m.targetMac);
    fields.scalar("target_ip").setString(m.targetIp);
    fields.scalar("is_gratuitous").setBoolean(m.isGratuitous);
    fields.scalar("is_probe").setBoolean(m.isProbe);
  }
}
