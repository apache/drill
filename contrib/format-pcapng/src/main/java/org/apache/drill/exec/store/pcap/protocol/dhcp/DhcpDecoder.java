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
package org.apache.drill.exec.store.pcap.protocol.dhcp;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.PacketProtocolDecoder;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * DHCP (RFC 2131, options from RFC 2132) over UDP ports 67 and 68. A payload is DHCP only if op is 1 or 2,
 * the hardware type is not 0, the hardware address length is at most 16 and the magic cookie follows the fixed BOOTP header. Plain
 * BOOTP without the cookie is left undecoded.
 */
public class DhcpDecoder implements PacketProtocolDecoder<DhcpMessage> {
  static final int MAX_ITEMS = 64;
  private static final int HEADER = 236;
  private static final int MAGIC_COOKIE = 0x63825363;
  private static final String[] MESSAGE_TYPES = {null, "DISCOVER", "OFFER", "REQUEST", "DECLINE", "ACK", "NAK",
      "RELEASE", "INFORM", "FORCERENEW", "LEASEQUERY", "LEASEUNASSIGNED", "LEASEUNKNOWN", "LEASEACTIVE"};

  @Override
  public String protocol() {
    return "dhcp";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("op", MinorType.VARCHAR)
        .addNullable("message_type", MinorType.VARCHAR)
        .addNullable("transaction_id", MinorType.BIGINT)
        .addNullable("broadcast", MinorType.BIT)
        .addNullable("client_mac", MinorType.VARCHAR)
        .addNullable("client_ip", MinorType.VARCHAR)
        .addNullable("your_ip", MinorType.VARCHAR)
        .addNullable("server_ip", MinorType.VARCHAR)
        .addNullable("relay_ip", MinorType.VARCHAR)
        .addNullable("server_name", MinorType.VARCHAR)
        .addNullable("boot_file", MinorType.VARCHAR)
        .addNullable("hostname", MinorType.VARCHAR)
        .addNullable("requested_ip", MinorType.VARCHAR)
        .addNullable("server_id", MinorType.VARCHAR)
        .addNullable("lease_time", MinorType.BIGINT)
        .addNullable("subnet_mask", MinorType.VARCHAR)
        .addNullable("vendor_class", MinorType.VARCHAR)
        .addNullable("domain_name", MinorType.VARCHAR)
        .addArray("routers", MinorType.VARCHAR)
        .addArray("dns_servers", MinorType.VARCHAR)
        .addArray("parameter_request_list", MinorType.INT)
        .addMapArray("options")
          .addNullable("code", MinorType.INT)
          .addNullable("value", MinorType.VARCHAR)
          .resumeSchema();
  }

  @Override
  public boolean accepts(Packet packet) {
    return packet.isUdpPacket() && (isDhcpPort(packet.getSrc_port()) || isDhcpPort(packet.getDst_port()));
  }

  private static boolean isDhcpPort(int port) {
    return port == 67 || port == 68;
  }

  @Override
  public DhcpMessage parse(Packet packet, byte[] payload, DecoderContext context) {
    if (payload == null || payload.length < HEADER + 4) {
      return null;
    }
    int op = payload[0] & 0xFF;
    int htype = payload[1] & 0xFF;
    int hlen = payload[2] & 0xFF;
    if ((op != 1 && op != 2) || htype == 0 || hlen > 16 || u32(payload, HEADER) != MAGIC_COOKIE) {
      return null;
    }
    DhcpMessage m = new DhcpMessage();
    m.op = op == 1 ? "request" : "reply";
    m.transactionId = u32(payload, 4) & 0xFFFFFFFFL;
    m.broadcast = (payload[10] & 0x80) != 0;
    m.clientIp = ipv4(payload, 12);
    m.yourIp = ipv4(payload, 16);
    m.serverIp = ipv4(payload, 20);
    m.relayIp = ipv4(payload, 24);
    m.clientMac = hlen == 0 ? null : mac(payload, 28, hlen);
    int overload = options(payload, m, context);
    // Option 52 says sname and/or file carry options instead of strings
    if ((overload & 2) == 0) {
      m.serverName = cString(payload, 44, 64);
    }
    if ((overload & 1) == 0) {
      m.bootFile = cString(payload, 108, 128);
    }
    return m;
  }

  /** Walks the options after the magic cookie; returns the value of option 52 (overload), or 0. */
  private static int options(byte[] b, DhcpMessage m, DecoderContext context) {
    int overload = 0;
    int pos = HEADER + 4;
    int count = 0;
    while (pos < b.length) {
      int code = b[pos] & 0xFF;
      if (code == 255) {
        break;
      }
      if (code == 0) {
        pos++;
        continue;
      }
      if (pos + 1 >= b.length) {
        throw new IllegalArgumentException("option " + code + " at offset " + pos + " has no length");
      }
      int len = b[pos + 1] & 0xFF;
      int at = pos + 2;
      if (at + len > b.length) {
        throw new IllegalArgumentException("option " + code + " at offset " + pos + " overruns the message");
      }
      if (count++ < MAX_ITEMS) {
        m.options.add(new DhcpMessage.Option(code, hex(b, at, len)));
      } else if (count == MAX_ITEMS + 1) {
        context.warn("options truncated to " + MAX_ITEMS);
      }
      switch (code) {
        case 1:
          m.subnetMask = ipv4(b, fixed(code, at, len, 4));
          break;
        case 3:
          addresses(b, code, at, len, m.routers, context);
          break;
        case 6:
          addresses(b, code, at, len, m.dnsServers, context);
          break;
        case 12:
          m.hostname = text(b, at, len);
          break;
        case 15:
          m.domainName = text(b, at, len);
          break;
        case 50:
          m.requestedIp = ipv4(b, fixed(code, at, len, 4));
          break;
        case 51:
          m.leaseTime = u32(b, fixed(code, at, len, 4)) & 0xFFFFFFFFL;
          break;
        case 52:
          overload = b[fixed(code, at, len, 1)] & 0x03;
          break;
        case 53:
          int type = b[fixed(code, at, len, 1)] & 0xFF;
          m.messageType = type < MESSAGE_TYPES.length && MESSAGE_TYPES[type] != null
              ? MESSAGE_TYPES[type] : String.valueOf(type);
          break;
        case 54:
          m.serverId = ipv4(b, fixed(code, at, len, 4));
          break;
        case 55:
          for (int i = 0; i < len && m.parameterRequestList.size() < MAX_ITEMS; i++) {
            m.parameterRequestList.add(b[at + i] & 0xFF);
          }
          break;
        case 60:
          m.vendorClass = text(b, at, len);
          break;
        default:
          break;
      }
      pos = at + len;
    }
    return overload;
  }

  private static int fixed(int code, int at, int len, int expected) {
    if (len != expected) {
      throw new IllegalArgumentException("option " + code + " has length " + len);
    }
    return at;
  }

  private static void addresses(byte[] b, int code, int at, int len, List<String> out, DecoderContext context) {
    if (len == 0 || len % 4 != 0) {
      throw new IllegalArgumentException("option " + code + " has length " + len);
    }
    for (int i = 0; i < len; i += 4) {
      if (out.size() == MAX_ITEMS) {
        context.warn("option " + code + " addresses truncated to " + MAX_ITEMS);
        return;
      }
      out.add(ipv4(b, at + i));
    }
  }

  /** Option text, without the trailing NULs some clients send; null if empty. */
  private static String text(byte[] b, int at, int len) {
    int end = at + len;
    while (end > at && b[end - 1] == 0) {
      end--;
    }
    return end == at ? null : new String(b, at, end - at, StandardCharsets.UTF_8);
  }

  /** A NUL-terminated string in a fixed-size field; null if empty. */
  private static String cString(byte[] b, int at, int size) {
    int end = at;
    while (end < at + size && b[end] != 0) {
      end++;
    }
    return end == at ? null : new String(b, at, end - at, StandardCharsets.ISO_8859_1);
  }

  private static String ipv4(byte[] b, int at) {
    return (b[at] & 0xFF) + "." + (b[at + 1] & 0xFF) + "." + (b[at + 2] & 0xFF) + "." + (b[at + 3] & 0xFF);
  }

  private static String mac(byte[] b, int at, int len) {
    StringBuilder out = new StringBuilder();
    for (int i = 0; i < len; i++) {
      if (i > 0) {
        out.append(':');
      }
      out.append(String.format("%02x", b[at + i]));
    }
    return out.toString();
  }

  private static String hex(byte[] b, int at, int len) {
    StringBuilder out = new StringBuilder();
    for (int i = at; i < at + len; i++) {
      out.append(String.format("%02x", b[i]));
    }
    return out.toString();
  }

  private static int u32(byte[] b, int at) {
    return ((b[at] & 0xFF) << 24) | ((b[at + 1] & 0xFF) << 16) | ((b[at + 2] & 0xFF) << 8) | (b[at + 3] & 0xFF);
  }

  @Override
  public void write(DhcpMessage m, TupleWriter fields) {
    set(fields, "op", m.op);
    set(fields, "message_type", m.messageType);
    fields.scalar("transaction_id").setLong(m.transactionId);
    fields.scalar("broadcast").setBoolean(m.broadcast);
    set(fields, "client_mac", m.clientMac);
    set(fields, "client_ip", m.clientIp);
    set(fields, "your_ip", m.yourIp);
    set(fields, "server_ip", m.serverIp);
    set(fields, "relay_ip", m.relayIp);
    set(fields, "server_name", m.serverName);
    set(fields, "boot_file", m.bootFile);
    set(fields, "hostname", m.hostname);
    set(fields, "requested_ip", m.requestedIp);
    set(fields, "server_id", m.serverId);
    if (m.leaseTime != null) {
      fields.scalar("lease_time").setLong(m.leaseTime);
    }
    set(fields, "subnet_mask", m.subnetMask);
    set(fields, "vendor_class", m.vendorClass);
    set(fields, "domain_name", m.domainName);
    ScalarWriter routers = fields.array("routers").scalar();
    m.routers.forEach(routers::setString);
    ScalarWriter dns = fields.array("dns_servers").scalar();
    m.dnsServers.forEach(dns::setString);
    ScalarWriter prl = fields.array("parameter_request_list").scalar();
    m.parameterRequestList.forEach(prl::setInt);
    ArrayWriter options = fields.array("options");
    for (DhcpMessage.Option o : m.options) {
      TupleWriter t = options.tuple();
      t.scalar("code").setInt(o.code);
      t.scalar("value").setString(o.value);
      options.save();
    }
  }

  private static void set(TupleWriter fields, String name, String value) {
    if (value != null) {
      fields.scalar(name).setString(value);
    }
  }
}
