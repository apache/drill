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
package org.apache.drill.exec.store.pcap.protocol.dhcpv6;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.PacketProtocolDecoder;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * DHCPv6 (RFC 8415) over UDP ports 546 and 547. A payload is DHCPv6 only if its message type is assigned
 * and its first option fits in the payload. Relay messages are unwrapped: the relay fields describe the
 * outermost relay, and the other fields describe the innermost relayed client or server message.
 */
public class Dhcpv6Decoder implements PacketProtocolDecoder<Dhcpv6Message> {
  static final int MAX_ITEMS = 64;
  private static final int MAX_STRING = 4096;
  private static final int MAX_RELAY_DEPTH = 8;
  private static final int RELAY_FORW = 12;
  private static final int RELAY_REPL = 13;
  private static final String[] MESSAGE_TYPES = {null, "SOLICIT", "ADVERTISE", "REQUEST", "CONFIRM", "RENEW",
      "REBIND", "REPLY", "RELEASE", "DECLINE", "RECONFIGURE", "INFORMATION-REQUEST", "RELAY-FORW", "RELAY-REPL",
      "LEASEQUERY", "LEASEQUERY-REPLY", "LEASEQUERY-DONE", "LEASEQUERY-DATA"};

  /** Thrown inside the parser when the payload turns out not to be DHCPv6. */
  private static final class NotDhcpv6 extends RuntimeException {
    NotDhcpv6() {
      super(null, null, false, false);
    }
  }

  private interface OptionHandler {
    void handle(int code, int at, int len);
  }

  @Override
  public String protocol() {
    return "dhcpv6";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("message_type", MinorType.VARCHAR)
        .addNullable("relayed_message_type", MinorType.VARCHAR)
        .addNullable("hop_count", MinorType.INT)
        .addNullable("link_address", MinorType.VARCHAR)
        .addNullable("peer_address", MinorType.VARCHAR)
        .addNullable("transaction_id", MinorType.INT)
        .addNullable("client_duid", MinorType.VARCHAR)
        .addNullable("server_duid", MinorType.VARCHAR)
        .addNullable("status_code", MinorType.INT)
        .addNullable("status_message", MinorType.VARCHAR)
        .addNullable("fqdn", MinorType.VARCHAR)
        .addArray("ia_addresses", MinorType.VARCHAR)
        .addArray("ia_prefixes", MinorType.VARCHAR)
        .addArray("dns_servers", MinorType.VARCHAR)
        .addArray("domain_list", MinorType.VARCHAR)
        .addArray("option_request_list", MinorType.INT)
        .addMapArray("options")
          .addNullable("code", MinorType.INT)
          .addNullable("value", MinorType.VARCHAR)
          .resumeSchema();
  }

  @Override
  public boolean accepts(Packet packet) {
    return packet.isUdpPacket() && (isDhcpv6Port(packet.getSrc_port()) || isDhcpv6Port(packet.getDst_port()));
  }

  private static boolean isDhcpv6Port(int port) {
    return port == 546 || port == 547;
  }

  @Override
  public Dhcpv6Message parse(Packet packet, byte[] payload, DecoderContext context) {
    if (payload == null || payload.length < 4) {
      return null;
    }
    Dhcpv6Message m = new Dhcpv6Message();
    try {
      new Parser(payload, m, context).message(0, payload.length, 0);
    } catch (NotDhcpv6 e) {
      return null;
    }
    return m;
  }

  /** Parser state for one payload. */
  private static final class Parser {
    private final byte[] b;
    private final Dhcpv6Message m;
    private final DecoderContext context;
    // False until the first option of the outermost message has been read
    private boolean confident;
    // Fields already reported as truncated
    private final Set<String> capped = new HashSet<>();

    Parser(byte[] b, Dhcpv6Message m, DecoderContext context) {
      this.b = b;
      this.m = m;
      this.context = context;
    }

    void message(int start, int end, int depth) {
      if (end - start < 4) {
        throw new IllegalArgumentException("truncated relayed message at offset " + start);
      }
      int type = b[start] & 0xFF;
      if (type == 0 || type >= MESSAGE_TYPES.length) {
        if (!confident) {
          throw new NotDhcpv6();
        }
        throw new IllegalArgumentException("relayed message type " + type + " is not valid");
      }
      if (depth == 0) {
        m.messageType = MESSAGE_TYPES[type];
      } else {
        m.relayedMessageType = MESSAGE_TYPES[type];
      }
      if (type == RELAY_FORW || type == RELAY_REPL) {
        relay(start, end, depth);
        return;
      }
      m.transactionId = ((b[start + 1] & 0xFF) << 16) | ((b[start + 2] & 0xFF) << 8) | (b[start + 3] & 0xFF);
      int count = walk(start + 4, end, this::option);
      if (count == 0 && !confident) {
        throw new NotDhcpv6();
      }
    }

    private void relay(int start, int end, int depth) {
      if (end - start < 34) {
        if (!confident) {
          throw new NotDhcpv6();
        }
        throw new IllegalArgumentException("truncated relay message at offset " + start);
      }
      if (depth == MAX_RELAY_DEPTH) {
        throw new IllegalArgumentException("relay messages nested deeper than " + MAX_RELAY_DEPTH);
      }
      if (depth == 0) {
        m.hopCount = b[start + 1] & 0xFF;
        m.linkAddress = ipv6(start + 2);
        m.peerAddress = ipv6(start + 18);
      }
      int[] relayed = {-1, 0};
      int count = walk(start + 34, end, (code, at, len) -> {
        if (code == 9 && relayed[0] < 0) {
          relayed[0] = at;
          relayed[1] = len;
        }
      });
      if (count == 0 && !confident) {
        throw new NotDhcpv6();
      }
      if (relayed[0] < 0) {
        throw new IllegalArgumentException("relay message has no relayed message option");
      }
      message(relayed[0], relayed[0] + relayed[1], depth + 1);
    }

    /** Calls handler for each option in [start, end); returns the number of options. */
    private int walk(int start, int end, OptionHandler handler) {
      int pos = start;
      int count = 0;
      while (pos < end) {
        if (pos + 4 > end) {
          notConfident();
          throw new IllegalArgumentException("truncated option header at offset " + pos);
        }
        int code = u16(pos);
        int len = u16(pos + 2);
        if (pos + 4 + len > end) {
          notConfident();
          throw new IllegalArgumentException("option " + code + " at offset " + pos + " overruns the message");
        }
        if (code == 0) {
          notConfident();
        }
        confident = true;
        handler.handle(code, pos + 4, len);
        count++;
        pos += 4 + len;
      }
      return count;
    }

    private void notConfident() {
      if (!confident) {
        throw new NotDhcpv6();
      }
    }

    /** A top-level option of a client or server message. */
    private void option(int code, int at, int len) {
      if (m.options.size() < MAX_ITEMS) {
        m.options.add(new Dhcpv6Message.Option(code, hex(at, len)));
      } else if (capped.add("options")) {
        context.warn("options truncated to " + MAX_ITEMS);
      }
      switch (code) {
        case 1:
          m.clientDuid = hex(at, len);
          break;
        case 2:
          m.serverDuid = hex(at, len);
          break;
        case 3:
        case 4:
        case 25:
          identityAssociation(code, at, len);
          break;
        case 6:
          minimum(code, len, 0, 2);
          for (int i = 0; i < len && m.optionRequestList.size() < MAX_ITEMS; i += 2) {
            m.optionRequestList.add(u16(at + i));
          }
          break;
        case 13:
          minimum(code, len, 2, 1);
          m.statusCode = u16(at);
          m.statusMessage = len > 2 ? cap(new String(b, at + 2, len - 2, StandardCharsets.UTF_8)) : null;
          break;
        case 23:
          minimum(code, len, 0, 16);
          for (int i = 0; i < len; i += 16) {
            add(m.dnsServers, ipv6(at + i), "dns_servers");
          }
          break;
        case 24:
          int pos = at;
          while (pos < at + len) {
            int[] next = new int[1];
            add(m.domainList, name(code, pos, at + len, next), "domain_list");
            pos = next[0];
          }
          break;
        case 39:
          minimum(code, len, 1, 1);
          m.fqdn = len > 1 ? name(code, at + 1, at + len, new int[1]) : null;
          break;
        default:
          break;
      }
    }

    /** IA_NA (3), IA_TA (4) or IA_PD (25), and their IA Address (5) and IA Prefix (26) options. */
    private void identityAssociation(int code, int at, int len) {
      int header = code == 4 ? 4 : 12;
      minimum(code, len, header, 1);
      walk(at + header, at + len, (sub, subAt, subLen) -> {
        if (sub == 5) {
          minimum(sub, subLen, 24, 1);
          add(m.iaAddresses, ipv6(subAt), "ia_addresses");
        } else if (sub == 26) {
          minimum(sub, subLen, 25, 1);
          add(m.iaPrefixes, ipv6(subAt + 9) + "/" + (b[subAt + 8] & 0xFF), "ia_prefixes");
        }
      });
    }

    /** Checks len is at least min and a multiple of unit. */
    private static void minimum(int code, int len, int min, int unit) {
      if (len < min || len % unit != 0) {
        throw new IllegalArgumentException("option " + code + " has length " + len);
      }
    }

    private void add(List<String> list, String value, String field) {
      if (list.size() < MAX_ITEMS) {
        list.add(value);
      } else if (capped.add(field)) {
        context.warn(field + " truncated to " + MAX_ITEMS);
      }
    }

    /**
     * An uncompressed DNS name (RFC 1035 3.1) in [at, limit). A name that reaches limit without a root
     * label is a partial name, which RFC 4704 allows in the FQDN option.
     */
    private String name(int code, int at, int limit, int[] next) {
      StringBuilder out = new StringBuilder();
      int p = at;
      while (p < limit) {
        int len = b[p] & 0xFF;
        if (len == 0) {
          p++;
          break;
        }
        if (len > 63 || p + 1 + len > limit) {
          throw new IllegalArgumentException("option " + code + " has an invalid domain name label");
        }
        if (out.length() > 0) {
          out.append('.');
        }
        out.append(new String(b, p + 1, len, StandardCharsets.ISO_8859_1));
        if (out.length() > 255) {
          throw new IllegalArgumentException("option " + code + " has a domain name longer than 255 bytes");
        }
        p += 1 + len;
      }
      next[0] = p;
      return out.length() == 0 ? "." : out.toString();
    }

    private String ipv6(int at) {
      try {
        return InetAddress.getByAddress(Arrays.copyOfRange(b, at, at + 16)).getHostAddress();
      } catch (UnknownHostException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }

    private String hex(int at, int len) {
      StringBuilder out = new StringBuilder();
      for (int i = at; i < at + len && out.length() < MAX_STRING; i++) {
        out.append(String.format("%02x", b[i]));
      }
      return out.toString();
    }

    private static String cap(String s) {
      return s.length() > MAX_STRING ? s.substring(0, MAX_STRING) : s;
    }

    private int u16(int at) {
      return ((b[at] & 0xFF) << 8) | (b[at + 1] & 0xFF);
    }
  }

  @Override
  public void write(Dhcpv6Message m, TupleWriter fields) {
    set(fields, "message_type", m.messageType);
    set(fields, "relayed_message_type", m.relayedMessageType);
    setInt(fields, "hop_count", m.hopCount);
    set(fields, "link_address", m.linkAddress);
    set(fields, "peer_address", m.peerAddress);
    setInt(fields, "transaction_id", m.transactionId);
    set(fields, "client_duid", m.clientDuid);
    set(fields, "server_duid", m.serverDuid);
    setInt(fields, "status_code", m.statusCode);
    set(fields, "status_message", m.statusMessage);
    set(fields, "fqdn", m.fqdn);
    strings(fields, "ia_addresses", m.iaAddresses);
    strings(fields, "ia_prefixes", m.iaPrefixes);
    strings(fields, "dns_servers", m.dnsServers);
    strings(fields, "domain_list", m.domainList);
    ScalarWriter oro = fields.array("option_request_list").scalar();
    m.optionRequestList.forEach(oro::setInt);
    ArrayWriter options = fields.array("options");
    for (Dhcpv6Message.Option o : m.options) {
      TupleWriter t = options.tuple();
      t.scalar("code").setInt(o.code);
      t.scalar("value").setString(o.value);
      options.save();
    }
  }

  private static void strings(TupleWriter fields, String name, List<String> values) {
    ScalarWriter writer = fields.array(name).scalar();
    values.forEach(writer::setString);
  }

  private static void set(TupleWriter fields, String name, String value) {
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
