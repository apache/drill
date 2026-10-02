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
package org.apache.drill.exec.store.pcap.protocol.netbios;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;

/** Parses NetBIOS Name Service messages (RFC 1002, section 4.2) with bounded work. */
public final class NetbiosNsParser {
  static final int MAX_ITEMS = 64;
  private static final int MAX_POINTER_JUMPS = 16;
  private static final int ENCODED_LENGTH = 32;
  private static final int TYPE_A = 0x01;
  private static final int TYPE_NB = 0x20;
  private static final int TYPE_NBSTAT = 0x21;
  private static final Map<Integer, String> OPCODES = new HashMap<>();
  private static final Map<Integer, String> SUFFIXES = new HashMap<>();
  private static final String[] NODE_TYPES = {"B", "P", "M", "H"};

  static {
    OPCODES.put(0, "query");
    OPCODES.put(5, "registration");
    OPCODES.put(6, "release");
    OPCODES.put(7, "wack");
    OPCODES.put(8, "refresh");
    OPCODES.put(9, "refresh");
    OPCODES.put(15, "multihomed_registration");
    Object[][] suffixes = {{0x00, "workstation"}, {0x01, "browser"}, {0x03, "messenger"}, {0x06, "ras_server"},
        {0x1B, "domain_master"}, {0x1C, "domain_controllers"}, {0x1D, "master_browser"},
        {0x1E, "browser_election"}, {0x1F, "netdde"}, {0x20, "file_server"}, {0x21, "ras_client"},
        {0xBE, "network_monitor_agent"}, {0xBF, "network_monitor_analyzer"}};
    for (Object[] s : suffixes) {
      SUFFIXES.put((Integer) s[0], (String) s[1]);
    }
  }

  private final byte[] b;
  private final int end;
  private int pos;

  private NetbiosNsParser(byte[] b) {
    this.b = b;
    this.end = b.length;
  }

  /**
   * @return the message, or null if the data is not NetBIOS-NS
   * @throws IllegalArgumentException if the data is NetBIOS-NS but malformed
   */
  public static NetbiosNsMessage parse(byte[] data, DecoderContext context) {
    return new NetbiosNsParser(data).message(context);
  }

  private NetbiosNsMessage message(DecoderContext context) {
    if (end < 12 + 1 + ENCODED_LENGTH + 1) {
      return null;
    }
    int flags = u16(2);
    int qd = u16(4);
    int an = u16(6);
    int ns = u16(8);
    int ar = u16(10);
    String opcode = OPCODES.get((flags >> 11) & 0xF);
    int records = an + ns + ar;
    // Reserved bits 0x0060 are zero; questions need at least 6 bytes and records 12 after the first name
    if (opcode == null || (flags & 0x0060) != 0 || qd + records == 0 || qd * 6 + records * 12 > end - 12) {
      return null;
    }
    // The first name must be a valid encoded label; otherwise this is not NetBIOS-NS
    pos = 12;
    NbName first;
    try {
      first = name();
    } catch (IllegalArgumentException e) {
      return null;
    }
    NetbiosNsMessage m = new NetbiosNsMessage();
    m.transactionId = u16(0);
    m.isResponse = (flags & 0x8000) != 0;
    m.opcode = opcode;
    m.broadcast = (flags & 0x0010) != 0;
    m.rcode = flags & 0xF;
    for (int i = 0; i < qd; i++) {
      try {
        NbName n = i == 0 ? first : name();
        NetbiosNsMessage.Question q = new NetbiosNsMessage.Question();
        q.name = n.name;
        q.suffix = n.suffix;
        q.suffixName = SUFFIXES.get(n.suffix);
        q.type = type(u16(need(4)));
        if (i < MAX_ITEMS) {
          m.questions.add(q);
        }
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("truncated question " + (i + 1) + ": " + e.getMessage());
      }
    }
    if (qd > MAX_ITEMS) {
      context.warn("questions truncated to " + MAX_ITEMS);
    }
    String[] sections = {"answer", "authority", "additional"};
    int[] counts = {an, ns, ar};
    List<?>[] lists = {m.answers, m.authorities, m.additionals};
    boolean firstRecord = qd == 0;
    for (int s = 0; s < 3; s++) {
      for (int i = 0; i < counts[s]; i++) {
        if (i == MAX_ITEMS) {
          context.warn(sections[s] + "s truncated to " + MAX_ITEMS);
        }
        try {
          NetbiosNsMessage.Record record = record(firstRecord ? first : name(), context);
          firstRecord = false;
          if (i < MAX_ITEMS) {
            @SuppressWarnings("unchecked")
            List<NetbiosNsMessage.Record> list = (List<NetbiosNsMessage.Record>) lists[s];
            list.add(record);
          }
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException("truncated " + sections[s] + " " + (i + 1) + ": " + e.getMessage());
        }
      }
    }
    return m;
  }

  private NetbiosNsMessage.Record record(NbName n, DecoderContext context) {
    NetbiosNsMessage.Record r = new NetbiosNsMessage.Record();
    r.name = n.name;
    r.suffix = n.suffix;
    r.suffixName = SUFFIXES.get(n.suffix);
    int at = need(10);
    int type = u16(at);
    r.type = type(type);
    r.ttl = ((long) u16(at + 4) << 16) | u16(at + 6);
    int length = u16(at + 8);
    int data = need(length);
    switch (type) {
      case TYPE_NB:
        if (length % 6 != 0) {
          throw new IllegalArgumentException("bad NB record length " + length);
        }
        r.addresses = new ArrayList<>();
        for (int p = data; p < data + length; p += 6) {
          if (r.addresses.size() == MAX_ITEMS) {
            context.warn("addresses truncated to " + MAX_ITEMS);
            break;
          }
          int nbFlags = u16(p);
          if (p == data) {
            r.group = (nbFlags & 0x8000) != 0;
            r.nodeType = NODE_TYPES[(nbFlags >> 13) & 3];
          }
          r.addresses.add(ipv4(p + 2));
        }
        break;
      case TYPE_A:
        if (length != 4) {
          throw new IllegalArgumentException("bad A record length " + length);
        }
        r.addresses = new ArrayList<>();
        r.addresses.add(ipv4(data));
        break;
      case TYPE_NBSTAT:
        nodeStatus(r, data, length, context);
        break;
      default:
        break;
    }
    return r;
  }

  /** Node status: a count, 18 bytes per name, then statistics starting with the MAC address. */
  private void nodeStatus(NetbiosNsMessage.Record r, int data, int length, DecoderContext context) {
    if (length < 1) {
      throw new IllegalArgumentException("empty NBSTAT record");
    }
    int count = b[data] & 0xFF;
    if (1 + count * 18 > length) {
      throw new IllegalArgumentException("NBSTAT record too short for " + count + " names");
    }
    r.names = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      if (i == MAX_ITEMS) {
        context.warn("NBSTAT names truncated to " + MAX_ITEMS);
        break;
      }
      int p = data + 1 + i * 18;
      r.names.add(String.format("%s<%02x>", text(p), b[p + 15] & 0xFF));
    }
    int mac = data + 1 + count * 18;
    if (mac + 6 <= data + length) {
      StringBuilder out = new StringBuilder();
      for (int i = 0; i < 6; i++) {
        out.append(i == 0 ? "" : ":").append(String.format("%02x", b[mac + i]));
      }
      r.macAddress = out.toString();
    }
  }

  private static final class NbName {
    String name;
    int suffix;
  }

  /** Reads a name at pos and advances pos past it. */
  private NbName name() {
    StringBuilder scope = new StringBuilder();
    NbName n = null;
    int p = pos;
    int jumps = 0;
    int resume = -1;
    while (true) {
      if (p >= end) {
        throw new IllegalArgumentException("name runs past end of message");
      }
      int len = b[p] & 0xFF;
      if (len == 0) {
        p++;
        break;
      }
      if ((len & 0xC0) == 0xC0) {
        if (p + 1 >= end) {
          throw new IllegalArgumentException("truncated compression pointer");
        }
        int target = ((len & 0x3F) << 8) | (b[p + 1] & 0xFF);
        if (++jumps > MAX_POINTER_JUMPS || target >= p) {
          throw new IllegalArgumentException("invalid compression pointer");
        }
        if (resume < 0) {
          resume = p + 2;
        }
        p = target;
        continue;
      }
      if (len > 63 || p + 1 + len > end) {
        throw new IllegalArgumentException("invalid label");
      }
      if (n == null) {
        n = decodeFirstLevel(p, len);
      } else {
        scope.append('.').append(new String(b, p + 1, len, StandardCharsets.ISO_8859_1));
        if (scope.length() > 255) {
          throw new IllegalArgumentException("name longer than 255 bytes");
        }
      }
      p += 1 + len;
    }
    if (n == null) {
      throw new IllegalArgumentException("empty name");
    }
    n.name += scope;
    pos = resume >= 0 ? resume : p;
    return n;
  }

  /** Decodes the 32-character label at p (its length byte) into the 15-character name and the suffix. */
  private NbName decodeFirstLevel(int p, int len) {
    if (len != ENCODED_LENGTH) {
      throw new IllegalArgumentException("not an encoded NetBIOS name");
    }
    byte[] raw = new byte[16];
    for (int i = 0; i < 16; i++) {
      int hi = b[p + 1 + 2 * i] - 'A';
      int lo = b[p + 2 + 2 * i] - 'A';
      if (hi < 0 || hi > 15 || lo < 0 || lo > 15) {
        throw new IllegalArgumentException("not an encoded NetBIOS name");
      }
      raw[i] = (byte) ((hi << 4) | lo);
    }
    NbName n = new NbName();
    n.name = trim(new String(raw, 0, 15, StandardCharsets.ISO_8859_1));
    n.suffix = raw[15] & 0xFF;
    return n;
  }

  /** The 15-character name at p, without padding. */
  private String text(int p) {
    return trim(new String(b, p, 15, StandardCharsets.ISO_8859_1));
  }

  /** Strips the space and NUL padding. */
  private static String trim(String s) {
    int n = s.length();
    while (n > 0 && (s.charAt(n - 1) == ' ' || s.charAt(n - 1) == 0)) {
      n--;
    }
    return s.substring(0, n);
  }

  private String ipv4(int at) {
    return (b[at] & 0xFF) + "." + (b[at + 1] & 0xFF) + "." + (b[at + 2] & 0xFF) + "." + (b[at + 3] & 0xFF);
  }

  private static String type(int type) {
    switch (type) {
      case TYPE_A:
        return "A";
      case 0x02:
        return "NS";
      case 0x0A:
        return "NULL";
      case TYPE_NB:
        return "NB";
      case TYPE_NBSTAT:
        return "NBSTAT";
      default:
        return String.valueOf(type);
    }
  }

  /** Reserves count bytes at pos, returning their start. */
  private int need(int count) {
    if (pos + count > end) {
      throw new IllegalArgumentException("needs " + count + " bytes at offset " + pos);
    }
    int at = pos;
    pos += count;
    return at;
  }

  private int u16(int at) {
    if (at + 2 > end) {
      throw new IllegalArgumentException("truncated field at offset " + at);
    }
    return ((b[at] & 0xFF) << 8) | (b[at + 1] & 0xFF);
  }
}
