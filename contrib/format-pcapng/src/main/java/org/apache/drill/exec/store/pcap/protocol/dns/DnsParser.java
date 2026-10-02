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
package org.apache.drill.exec.store.pcap.protocol.dns;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;

/** Parses DNS messages (RFC 1035) with bounded work. */
public final class DnsParser {
  static final int MAX_ITEMS = 64;
  private static final int MAX_POINTER_JUMPS = 16;
  private static final int MAX_STRING = 4096;
  private static final Map<Integer, String> TYPES = new HashMap<>();

  static {
    String[][] types = {{"1", "A"}, {"2", "NS"}, {"5", "CNAME"}, {"6", "SOA"}, {"12", "PTR"}, {"13", "HINFO"},
        {"15", "MX"}, {"16", "TXT"}, {"28", "AAAA"}, {"33", "SRV"}, {"35", "NAPTR"}, {"39", "DNAME"},
        {"41", "OPT"}, {"43", "DS"}, {"46", "RRSIG"}, {"47", "NSEC"}, {"48", "DNSKEY"}, {"64", "SVCB"},
        {"65", "HTTPS"}, {"99", "SPF"}, {"252", "AXFR"}, {"255", "ANY"}, {"257", "CAA"}};
    for (String[] t : types) {
      TYPES.put(Integer.parseInt(t[0]), t[1]);
    }
  }

  private final byte[] b;
  private final int start;
  private final int end;
  private int pos;

  private DnsParser(byte[] b, int start) {
    this.b = b;
    this.start = start;
    this.end = b.length;
    this.pos = start;
  }

  /**
   * @return the message, or null if the data is not DNS
   * @throws IllegalArgumentException if the data is DNS but malformed
   */
  public static DnsMessage parse(byte[] data, int offset, DecoderContext context) {
    return new DnsParser(data, offset).message(context);
  }

  private DnsMessage message(DecoderContext context) {
    if (end - start < 12) {
      return null;
    }
    int flags = u16(start + 2);
    int qd = u16(start + 4);
    int an = u16(start + 6);
    int ns = u16(start + 8);
    int ar = u16(start + 10);
    DnsMessage m = new DnsMessage();
    m.transactionId = u16(start);
    m.isResponse = (flags & 0x8000) != 0;
    m.opcode = (flags >> 11) & 0xF;
    m.authoritative = (flags & 0x0400) != 0;
    m.truncated = (flags & 0x0200) != 0;
    m.recursionDesired = (flags & 0x0100) != 0;
    m.recursionAvailable = (flags & 0x0080) != 0;
    m.rcode = flags & 0xF;
    boolean zBit = (flags & 0x0040) != 0;
    int records = an + ns + ar;
    if (m.opcode > 6 || zBit || qd + records == 0 || qd * 5 + records * 11 > end - start - 12) {
      return null;
    }
    pos = start + 12;
    boolean confident = false;
    for (int i = 0; i < qd; i++) {
      try {
        DnsMessage.Question q = new DnsMessage.Question();
        q.name = name();
        q.type = type(u16(need(4)));
        q.dnsClass = u16(pos - 2);
        if (m.questions.size() < MAX_ITEMS) {
          m.questions.add(q);
        }
        confident = true;
      } catch (IllegalArgumentException e) {
        if (!confident) {
          return null;
        }
        throw new IllegalArgumentException("truncated question " + (i + 1) + ": " + e.getMessage());
      }
    }
    if (qd > MAX_ITEMS) {
      context.warn("questions truncated to " + MAX_ITEMS);
    }
    String[] sections = {"answer", "authority", "additional"};
    int[] counts = {an, ns, ar};
    List<?>[] lists = {m.answers, m.authorities, m.additionals};
    for (int s = 0; s < 3; s++) {
      for (int i = 0; i < counts[s]; i++) {
        if (i == MAX_ITEMS) {
          context.warn(sections[s] + "s truncated to " + MAX_ITEMS);
          return m;
        }
        try {
          @SuppressWarnings("unchecked")
          List<DnsMessage.Record> list = (List<DnsMessage.Record>) lists[s];
          list.add(record());
          confident = true;
        } catch (IllegalArgumentException e) {
          if (!confident) {
            return null;
          }
          throw new IllegalArgumentException("truncated " + sections[s] + " " + (i + 1) + ": " + e.getMessage());
        }
      }
    }
    return m;
  }

  private DnsMessage.Record record() {
    DnsMessage.Record r = new DnsMessage.Record();
    r.name = name();
    int at = need(10);
    int type = u16(at);
    r.type = type(type);
    r.dnsClass = u16(at + 2);
    r.ttl = u32(at + 4);
    int length = u16(at + 8);
    int data = need(length);
    r.data = rdata(type, data, length);
    return r;
  }

  private String rdata(int type, int at, int length) {
    switch (type) {
      case 1:
      case 28:
        if (length != (type == 1 ? 4 : 16)) {
          throw new IllegalArgumentException("bad address length " + length);
        }
        try {
          return InetAddress.getByAddress(Arrays.copyOfRange(b, at, at + length)).getHostAddress();
        } catch (UnknownHostException e) {
          throw new IllegalArgumentException(e.getMessage());
        }
      case 2:
      case 5:
      case 12:
      case 39:
        return nameAt(at, at + length);
      case 15:
        return u16(at) + " " + nameAt(at + 2, at + length);
      case 33:
        return u16(at) + " " + u16(at + 2) + " " + u16(at + 4) + " " + nameAt(at + 6, at + length);
      case 16:
        return text(at, at + length);
      default:
        return hex(at, length);
    }
  }

  /** Character strings joined as "a" "b", like dig. */
  private String text(int at, int limit) {
    StringBuilder out = new StringBuilder();
    int p = at;
    while (p < limit) {
      int len = b[p] & 0xFF;
      if (p + 1 + len > limit) {
        throw new IllegalArgumentException("bad TXT string length");
      }
      if (out.length() > 0) {
        out.append(' ');
      }
      out.append('"').append(new String(b, p + 1, len, StandardCharsets.ISO_8859_1)).append('"');
      p += 1 + len;
    }
    return cap(out.toString());
  }

  private String hex(int at, int length) {
    StringBuilder out = new StringBuilder();
    for (int i = at; i < at + length && out.length() < MAX_STRING; i++) {
      out.append(String.format("%02x", b[i]));
    }
    return cap(out.toString());
  }

  /** Reads a name at pos and advances pos past it. */
  private String name() {
    int[] next = new int[1];
    String n = readName(pos, next);
    pos = next[0];
    return n;
  }

  /** A name inside RDATA, which must fit before limit. */
  private String nameAt(int at, int limit) {
    int[] next = new int[1];
    String n = readName(at, next);
    if (next[0] > limit) {
      throw new IllegalArgumentException("name overruns record data");
    }
    return n;
  }

  /** next[0] receives the position after the name in the original sequence. */
  private String readName(int at, int[] next) {
    StringBuilder out = new StringBuilder();
    int p = at;
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
        int target = start + (((len & 0x3F) << 8) | (b[p + 1] & 0xFF));
        if (++jumps > MAX_POINTER_JUMPS || target >= p) {
          throw new IllegalArgumentException("invalid compression pointer");
        }
        if (resume < 0) {
          resume = p + 2;
        }
        p = target;
        continue;
      }
      if ((len & 0xC0) != 0 || len > 63 || p + 1 + len > end) {
        throw new IllegalArgumentException("invalid label");
      }
      if (out.length() > 0) {
        out.append('.');
      }
      out.append(new String(b, p + 1, len, StandardCharsets.ISO_8859_1));
      if (out.length() > 255) {
        throw new IllegalArgumentException("name longer than 255 bytes");
      }
      p += 1 + len;
    }
    next[0] = resume >= 0 ? resume : p;
    return out.length() == 0 ? "." : out.toString();
  }

  private static String type(int type) {
    String name = TYPES.get(type);
    return name != null ? name : String.valueOf(type);
  }

  private static String cap(String s) {
    return s.length() > MAX_STRING ? s.substring(0, MAX_STRING) : s;
  }

  /** Reserves count bytes at pos, returning their start. */
  private int need(int count) {
    if (pos + count > end) {
      throw new IllegalArgumentException("needs " + count + " bytes at offset " + (pos - start));
    }
    int at = pos;
    pos += count;
    return at;
  }

  private int u16(int at) {
    if (at + 2 > end) {
      throw new IllegalArgumentException("truncated field at offset " + (at - start));
    }
    return ((b[at] & 0xFF) << 8) | (b[at + 1] & 0xFF);
  }

  private long u32(int at) {
    return ((long) u16(at) << 16) | u16(at + 2);
  }
}
