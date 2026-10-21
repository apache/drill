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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.PacketProtocolDecoder;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * ICMP (IPv4 protocol 1) and ICMPv6 (IPv6 next header 58). The message is the IP payload, after any
 * IPv6 extension headers. Checksums are not verified.
 */
public class IcmpDecoder implements PacketProtocolDecoder<IcmpMessage> {
  private static final int HEADER = 4;
  /** Most extension headers walked in an embedded IPv6 packet. */
  private static final int MAX_EXTENSION_HEADERS = 8;

  /** Name and length of the fixed part, by type. */
  private static final class Type {
    final String name;
    final int fixedLength;

    Type(String name, int fixedLength) {
      this.name = name;
      this.fixedLength = fixedLength;
    }
  }

  private static final Map<Integer, Type> V4_TYPES = new HashMap<>();
  private static final Map<Integer, Type> V6_TYPES = new HashMap<>();
  private static final Map<Integer, String[]> V4_CODES = new HashMap<>();
  private static final Map<Integer, String[]> V6_CODES = new HashMap<>();

  static {
    v4(0, "echo_reply", 8);
    v4(3, "destination_unreachable", 8);
    v4(4, "source_quench", 8);
    v4(5, "redirect", 8);
    v4(8, "echo_request", 8);
    v4(9, "router_advertisement", 8);
    v4(10, "router_solicitation", 8);
    v4(11, "time_exceeded", 8);
    v4(12, "parameter_problem", 8);
    v4(13, "timestamp_request", 20);
    v4(14, "timestamp_reply", 20);
    v4(15, "information_request", 8);
    v4(16, "information_reply", 8);
    v4(17, "address_mask_request", 12);
    v4(18, "address_mask_reply", 12);
    v4(42, "extended_echo_request", 8);
    v4(43, "extended_echo_reply", 8);
    V4_CODES.put(3, new String[] {"net_unreachable", "host_unreachable", "protocol_unreachable",
        "port_unreachable", "fragmentation_needed", "source_route_failed", "destination_network_unknown",
        "destination_host_unknown", "source_host_isolated", "network_administratively_prohibited",
        "host_administratively_prohibited", "network_unreachable_for_tos", "host_unreachable_for_tos",
        "communication_administratively_prohibited", "host_precedence_violation", "precedence_cutoff_in_effect"});
    V4_CODES.put(5, new String[] {"redirect_for_network", "redirect_for_host", "redirect_for_tos_and_network",
        "redirect_for_tos_and_host"});
    V4_CODES.put(11, new String[] {"ttl_exceeded_in_transit", "fragment_reassembly_time_exceeded"});
    V4_CODES.put(12, new String[] {"pointer_indicates_error", "missing_required_option", "bad_length"});

    v6(1, "destination_unreachable", 8);
    v6(2, "packet_too_big", 8);
    v6(3, "time_exceeded", 8);
    v6(4, "parameter_problem", 8);
    v6(128, "echo_request", 8);
    v6(129, "echo_reply", 8);
    v6(130, "multicast_listener_query", 24);
    v6(131, "multicast_listener_report", 24);
    v6(132, "multicast_listener_done", 24);
    v6(133, "router_solicitation", 8);
    v6(134, "router_advertisement", 16);
    v6(135, "neighbor_solicitation", 24);
    v6(136, "neighbor_advertisement", 24);
    v6(137, "redirect", 40);
    v6(143, "multicast_listener_report_v2", 8);
    V6_CODES.put(1, new String[] {"no_route_to_destination", "administratively_prohibited",
        "beyond_scope_of_source_address", "address_unreachable", "port_unreachable",
        "source_address_failed_policy", "reject_route_to_destination", "error_in_source_routing_header"});
    V6_CODES.put(3, new String[] {"hop_limit_exceeded_in_transit", "fragment_reassembly_time_exceeded"});
    V6_CODES.put(4, new String[] {"erroneous_header_field", "unrecognized_next_header", "unrecognized_ipv6_option"});
  }

  private static void v4(int type, String name, int fixedLength) {
    V4_TYPES.put(type, new Type(name, fixedLength));
  }

  private static void v6(int type, String name, int fixedLength) {
    V6_TYPES.put(type, new Type(name, fixedLength));
  }

  @Override
  public String protocol() {
    return "icmp";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("version", MinorType.INT)
        .addNullable("type", MinorType.INT)
        .addNullable("code", MinorType.INT)
        .addNullable("type_name", MinorType.VARCHAR)
        .addNullable("code_name", MinorType.VARCHAR)
        .addNullable("identifier", MinorType.INT)
        .addNullable("sequence", MinorType.INT)
        .addNullable("mtu", MinorType.INT)
        .addNullable("gateway", MinorType.VARCHAR)
        .addNullable("target_address", MinorType.VARCHAR)
        .addNullable("destination_address", MinorType.VARCHAR)
        .addNullable("original_src_ip", MinorType.VARCHAR)
        .addNullable("original_dst_ip", MinorType.VARCHAR)
        .addNullable("original_protocol", MinorType.INT)
        .addNullable("original_src_port", MinorType.INT)
        .addNullable("original_dst_port", MinorType.INT);
  }

  @Override
  public boolean accepts(Packet packet) {
    return packet.isIcmpPacket();
  }

  @Override
  public IcmpMessage parse(Packet packet, byte[] payload, DecoderContext context) {
    // payload is the TCP or UDP payload, null here; the ICMP message is the IP payload
    return parse(packet.getIpPayload(), packet.isIpV6Packet() ? 6 : 4);
  }

  /**
   * @param b the ICMP message
   * @param version 4 for ICMP, 6 for ICMPv6
   * @return null if there are fewer than 4 bytes
   * @throws IllegalArgumentException if the fixed part of a known type is cut short
   */
  public static IcmpMessage parse(byte[] b, int version) {
    if (b == null || b.length < HEADER) {
      return null;
    }
    boolean v6 = version == 6;
    IcmpMessage m = new IcmpMessage();
    m.version = version;
    m.type = b[0] & 0xFF;
    m.code = b[1] & 0xFF;
    Type type = (v6 ? V6_TYPES : V4_TYPES).get(m.type);
    if (type == null) {
      return m;
    }
    m.typeName = type.name;
    if (b.length < type.fixedLength) {
      throw new IllegalArgumentException(
          String.format("truncated %s: %d bytes, needs %d", type.name, b.length, type.fixedLength));
    }
    String[] codes = (v6 ? V6_CODES : V4_CODES).get(m.type);
    if (codes != null && m.code < codes.length) {
      m.codeName = codes[m.code];
    }
    if (v6) {
      parseV6(b, m);
    } else {
      parseV4(b, m);
    }
    return m;
  }

  private static void parseV4(byte[] b, IcmpMessage m) {
    switch (m.type) {
      case 0: case 8: case 13: case 14: case 15: case 16: case 17: case 18:
        m.identifier = u16(b, 4);
        m.sequence = u16(b, 6);
        break;
      case 3:
        if (m.code == 4) {
          m.mtu = u16(b, 6);
        }
        embeddedV4(b, m);
        break;
      case 5:
        m.gateway = address(b, 4, 4);
        embeddedV4(b, m);
        break;
      case 4: case 11: case 12:
        embeddedV4(b, m);
        break;
      default:
        break;
    }
  }

  private static void parseV6(byte[] b, IcmpMessage m) {
    switch (m.type) {
      case 128: case 129:
        m.identifier = u16(b, 4);
        m.sequence = u16(b, 6);
        break;
      case 2:
        m.mtu = (int) Math.min(Integer.MAX_VALUE, ((long) u16(b, 4) << 16) | u16(b, 6));
        embeddedV6(b, m);
        break;
      case 1: case 3: case 4:
        embeddedV6(b, m);
        break;
      case 135: case 136:
        m.targetAddress = address(b, 8, 16);
        break;
      case 137:
        m.targetAddress = address(b, 8, 16);
        m.destinationAddress = address(b, 24, 16);
        break;
      default:
        break;
    }
  }

  /** The IPv4 header and transport ports of the packet that caused an error, as far as they were captured. */
  private static void embeddedV4(byte[] b, IcmpMessage m) {
    int ip = 8;
    if (b.length < ip + 20 || (b[ip] & 0xF0) != 0x40) {
      return;
    }
    int headerLength = (b[ip] & 0x0F) * 4;
    if (headerLength < 20 || ip + headerLength > b.length) {
      return;
    }
    m.originalSrcIp = address(b, ip + 12, 4);
    m.originalDstIp = address(b, ip + 16, 4);
    m.originalProtocol = b[ip + 9] & 0xFF;
    boolean firstFragment = (u16(b, ip + 6) & 0x1FFF) == 0;
    if (firstFragment) {
      ports(b, ip + headerLength, m);
    }
  }

  /** The IPv6 header and transport ports of the packet that caused an error, as far as they were captured. */
  private static void embeddedV6(byte[] b, IcmpMessage m) {
    int ip = 8;
    if (b.length < ip + 40 || (b[ip] & 0xF0) != 0x60) {
      return;
    }
    m.originalSrcIp = address(b, ip + 8, 16);
    m.originalDstIp = address(b, ip + 24, 16);
    int next = b[ip + 6] & 0xFF;
    int pos = ip + 40;
    boolean firstFragment = true;
    for (int i = 0; i < MAX_EXTENSION_HEADERS; i++) {
      int length;
      switch (next) {
        case 0: case 43: case 60: case 135: case 139: case 140:
          length = pos + 2 <= b.length ? ((b[pos + 1] & 0xFF) + 1) * 8 : -1;
          break;
        case 44:
          length = pos + 4 <= b.length ? 8 : -1;
          if (length > 0 && (u16(b, pos + 2) >>> 3) != 0) {
            firstFragment = false;
          }
          break;
        case 51:
          length = pos + 2 <= b.length ? ((b[pos + 1] & 0xFF) + 2) * 4 : -1;
          break;
        default:
          // An upper-layer protocol
          m.originalProtocol = next;
          if (firstFragment) {
            ports(b, pos, m);
          }
          return;
      }
      if (length < 0) {
        // The extension header was not captured
        return;
      }
      next = b[pos] & 0xFF;
      pos += length;
    }
  }

  private static void ports(byte[] b, int transport, IcmpMessage m) {
    int protocol = m.originalProtocol;
    // TCP, UDP, SCTP, UDP-Lite: ports are the first four bytes
    if ((protocol == 6 || protocol == 17 || protocol == 132 || protocol == 136) && transport + 4 <= b.length) {
      m.originalSrcPort = u16(b, transport);
      m.originalDstPort = u16(b, transport + 2);
    }
  }

  private static int u16(byte[] b, int offset) {
    return ((b[offset] & 0xFF) << 8) | (b[offset + 1] & 0xFF);
  }

  private static String address(byte[] b, int offset, int length) {
    try {
      return InetAddress.getByAddress(Arrays.copyOfRange(b, offset, offset + length)).getHostAddress();
    } catch (UnknownHostException e) {
      // Not reachable: the length is always 4 or 16
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void write(IcmpMessage m, TupleWriter fields) {
    fields.scalar("version").setInt(m.version);
    fields.scalar("type").setInt(m.type);
    fields.scalar("code").setInt(m.code);
    setString(fields, "type_name", m.typeName);
    setString(fields, "code_name", m.codeName);
    setInt(fields, "identifier", m.identifier);
    setInt(fields, "sequence", m.sequence);
    setInt(fields, "mtu", m.mtu);
    setString(fields, "gateway", m.gateway);
    setString(fields, "target_address", m.targetAddress);
    setString(fields, "destination_address", m.destinationAddress);
    setString(fields, "original_src_ip", m.originalSrcIp);
    setString(fields, "original_dst_ip", m.originalDstIp);
    setInt(fields, "original_protocol", m.originalProtocol);
    setInt(fields, "original_src_port", m.originalSrcPort);
    setInt(fields, "original_dst_port", m.originalDstPort);
  }

  private static void setString(TupleWriter fields, String name, String value) {
    if (value != null) {
      fields.scalar(name).setString(value);
    }
  }

  private static void setInt(TupleWriter fields, String name, Integer value) {
    if (value != null) {
      fields.scalar(name).setInt(value);
    }
  }
}
