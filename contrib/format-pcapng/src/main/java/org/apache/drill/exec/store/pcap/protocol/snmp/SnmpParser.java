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
package org.apache.drill.exec.store.pcap.protocol.snmp;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.snmp.BerReader.Element;

/** Parses SNMP v1, v2c and v3 messages (RFC 1157, RFC 3416, RFC 3412, RFC 3414) with bounded work. */
public final class SnmpParser {
  static final int MAX_ITEMS = 64;
  private static final int MAX_STRING = 4096;
  private static final int SEQUENCE = 0x30;
  private static final int INTEGER = 0x02;
  private static final int OCTET_STRING = 0x04;
  private static final int OID = 0x06;
  private static final int IP_ADDRESS = 0x40;
  private static final int TIMETICKS = 0x43;
  private static final int TRAP_V1 = 0xA4;
  private static final int GET_BULK = 0xA5;
  private static final String[] PDU_TYPES = {"get-request", "get-next-request", "get-response", "set-request",
      "trap", "get-bulk-request", "inform-request", "snmpv2-trap", "report"};
  private static final String[] SECURITY_LEVELS = {"noAuthNoPriv", "authNoPriv", null, "authPriv"};

  private final DecoderContext context;

  private SnmpParser(DecoderContext context) {
    this.context = context;
  }

  /**
   * @return the message, or null if the data is not SNMP
   * @throws IllegalArgumentException if the data is SNMP but malformed or truncated
   */
  public static SnmpMessage parse(byte[] b, DecoderContext context) {
    // Not SNMP unless it starts with a SEQUENCE header followed by INTEGER version 0, 1 or 3
    if (b.length < 2 || (b[0] & 0xFF) != SEQUENCE) {
      return null;
    }
    int first = b[1] & 0xFF;
    int contentStart;
    long length;
    if (first < 0x80) {
      contentStart = 2;
      length = first;
    } else {
      int count = first & 0x7F;
      if (count == 0 || count > 4 || 2 + count > b.length) {
        return null;
      }
      length = 0;
      for (int i = 0; i < count; i++) {
        length = (length << 8) | (b[2 + i] & 0xFF);
      }
      contentStart = 2 + count;
    }
    if (contentStart + 3 > b.length || b[contentStart] != INTEGER || b[contentStart + 1] != 1) {
      return null;
    }
    int version = b[contentStart + 2];
    if (version != 0 && version != 1 && version != 3) {
      return null;
    }
    if (contentStart + length > b.length) {
      throw new IllegalArgumentException("truncated: message length " + length + " but "
          + (b.length - contentStart) + " bytes");
    }
    if (contentStart + length < b.length) {
      throw new IllegalArgumentException((b.length - contentStart - length) + " bytes after the message");
    }
    BerReader message = new BerReader(b, contentStart + 3, b.length, 1);
    return new SnmpParser(context).message(message, version);
  }

  private SnmpMessage message(BerReader r, int version) {
    SnmpMessage m = new SnmpMessage();
    if (version == 3) {
      m.version = "v3";
      v3(r, m);
    } else {
      m.version = version == 0 ? "v1" : "v2c";
      Element community = r.expect(OCTET_STRING, "community");
      m.communityPresent = community.length > 0;
      if (context.exposeCredentials()) {
        m.community = text(r.bytes(), community);
      }
      if (!r.hasMore()) {
        throw new IllegalArgumentException("missing PDU");
      }
      pdu(r, r.next(), m);
    }
    return m;
  }

  private void v3(BerReader r, SnmpMessage m) {
    BerReader global = r.enter(r.expect(SEQUENCE, "msgGlobalData"));
    m.msgId = integer(global, "msgID");
    integer(global, "msgMaxSize");
    Element flags = global.expect(OCTET_STRING, "msgFlags");
    if (flags.length != 1) {
      throw new IllegalArgumentException("msgFlags has length " + flags.length);
    }
    long securityModel = integer(global, "msgSecurityModel");
    m.securityLevel = SECURITY_LEVELS[r.bytes()[flags.start] & 3];
    if (m.securityLevel == null) {
      context.warn("invalid msgFlags: privacy without authentication");
    }
    Element params = r.expect(OCTET_STRING, "msgSecurityParameters");
    if (securityModel == 3 && params.length > 0) {
      // User-based security model: the parameters are themselves a BER SEQUENCE
      BerReader wrapper = r.enter(params);
      BerReader usm = wrapper.enter(wrapper.expect(SEQUENCE, "UsmSecurityParameters"));
      m.engineId = hex(r.bytes(), usm.expect(OCTET_STRING, "msgAuthoritativeEngineID"));
      integer(usm, "msgAuthoritativeEngineBoots");
      integer(usm, "msgAuthoritativeEngineTime");
      m.msgUserName = text(r.bytes(), usm.expect(OCTET_STRING, "msgUserName"));
    }
    if (!r.hasMore()) {
      throw new IllegalArgumentException("missing msgData");
    }
    Element data = r.next();
    if (data.tag == OCTET_STRING) {
      // Encrypted scoped PDU: never decrypted
      m.encrypted = true;
    } else if (data.tag == SEQUENCE) {
      BerReader scoped = r.enter(data);
      scoped.expect(OCTET_STRING, "contextEngineID");
      scoped.expect(OCTET_STRING, "contextName");
      if (!scoped.hasMore()) {
        throw new IllegalArgumentException("missing PDU");
      }
      pdu(scoped, scoped.next(), m);
    } else {
      throw new IllegalArgumentException(String.format("unexpected msgData tag 0x%02x", data.tag));
    }
  }

  private void pdu(BerReader parent, Element e, SnmpMessage m) {
    if (e.tag < 0xA0 || e.tag > 0xA8) {
      throw new IllegalArgumentException(String.format("unknown PDU type 0x%02x", e.tag));
    }
    m.pduType = PDU_TYPES[e.tag - 0xA0];
    BerReader r = parent.enter(e);
    if (e.tag == TRAP_V1) {
      m.enterprise = oid(r.bytes(), r.expect(OID, "enterprise"));
      Element agent = r.expect(IP_ADDRESS, "agent-addr");
      if (agent.length != 4) {
        throw new IllegalArgumentException("agent-addr has length " + agent.length);
      }
      m.agentAddress = ipv4(r.bytes(), agent.start);
      m.genericTrap = toInt(integer(r, "generic-trap"), "generic-trap");
      m.specificTrap = integer(r, "specific-trap");
      m.timeStamp = unsigned(r.bytes(), r.expect(TIMETICKS, "time-stamp")).longValue();
    } else {
      m.requestId = integer(r, "request-id");
      int first = toInt(integer(r, "error-status"), "error-status");
      int second = toInt(integer(r, "error-index"), "error-index");
      if (e.tag == GET_BULK) {
        m.nonRepeaters = first;
        m.maxRepetitions = second;
      } else {
        m.errorStatus = first;
        m.errorIndex = second;
      }
    }
    varbinds(r.enter(r.expect(SEQUENCE, "variable-bindings")), m);
  }

  private void varbinds(BerReader list, SnmpMessage m) {
    int count = 0;
    while (list.hasMore()) {
      BerReader vb = list.enter(list.expect(SEQUENCE, "VarBind"));
      count++;
      String name = oid(vb.bytes(), vb.expect(OID, "VarBind name"));
      if (!vb.hasMore()) {
        throw new IllegalArgumentException("VarBind " + count + " has no value");
      }
      Element value = vb.next();
      if (count > MAX_ITEMS) {
        if (count == MAX_ITEMS + 1) {
          context.warn("varbinds truncated to " + MAX_ITEMS);
        }
        continue;
      }
      SnmpMessage.Varbind v = new SnmpMessage.Varbind();
      v.oid = name;
      value(vb.bytes(), value, v);
      m.varbinds.add(v);
    }
  }

  private static void value(byte[] b, Element e, SnmpMessage.Varbind v) {
    switch (e.tag) {
      case INTEGER:
        v.valueType = "integer";
        v.value = signed(b, e).toString();
        break;
      case OCTET_STRING:
        v.valueType = "octet_string";
        v.value = printable(b, e) ? text(b, e) : hex(b, e);
        break;
      case 0x05:
        v.valueType = "null";
        break;
      case OID:
        v.valueType = "oid";
        v.value = oid(b, e);
        break;
      case IP_ADDRESS:
        v.valueType = "ip_address";
        v.value = e.length == 4 ? ipv4(b, e.start) : hex(b, e);
        break;
      case 0x41:
        v.valueType = "counter32";
        v.value = unsigned(b, e).toString();
        break;
      case 0x42:
        v.valueType = "gauge32";
        v.value = unsigned(b, e).toString();
        break;
      case TIMETICKS:
        v.valueType = "timeticks";
        v.value = unsigned(b, e).toString();
        break;
      case 0x44:
        v.valueType = "opaque";
        v.value = hex(b, e);
        break;
      case 0x46:
        v.valueType = "counter64";
        v.value = unsigned(b, e).toString();
        break;
      case 0x80:
        v.valueType = "no_such_object";
        break;
      case 0x81:
        v.valueType = "no_such_instance";
        break;
      case 0x82:
        v.valueType = "end_of_mib_view";
        break;
      default:
        v.valueType = String.format("0x%02x", e.tag);
        v.value = hex(b, e);
        break;
    }
  }

  private static long integer(BerReader r, String what) {
    Element e = r.expect(INTEGER, what);
    if (e.length > 8) {
      throw new IllegalArgumentException(what + " is longer than 8 bytes");
    }
    return signed(r.bytes(), e).longValue();
  }

  private static int toInt(long v, String what) {
    if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(what + " out of range: " + v);
    }
    return (int) v;
  }

  private static BigInteger signed(byte[] b, Element e) {
    if (e.length < 1 || e.length > 9) {
      throw new IllegalArgumentException("bad integer length " + e.length);
    }
    return new BigInteger(Arrays.copyOfRange(b, e.start, e.end()));
  }

  /** Counter, gauge and timeticks values, read as unsigned even if the sender omitted the leading zero. */
  private static BigInteger unsigned(byte[] b, Element e) {
    if (e.length < 1 || e.length > 9) {
      throw new IllegalArgumentException("bad integer length " + e.length);
    }
    return new BigInteger(1, Arrays.copyOfRange(b, e.start, e.end()));
  }

  /** Dotted object identifier. */
  static String oid(byte[] b, Element e) {
    if (e.length == 0) {
      throw new IllegalArgumentException("empty OID");
    }
    StringBuilder out = new StringBuilder();
    long value = 0;
    int bytes = 0;
    boolean first = true;
    for (int i = e.start; i < e.end(); i++) {
      int octet = b[i] & 0xFF;
      if (++bytes > 9) {
        throw new IllegalArgumentException("OID sub-identifier too long");
      }
      value = (value << 7) | (octet & 0x7F);
      if ((octet & 0x80) != 0) {
        continue;
      }
      if (first) {
        int top = value < 80 ? (int) (value / 40) : 2;
        out.append(top).append('.').append(value - top * 40L);
        first = false;
      } else if (out.length() < MAX_STRING) {
        out.append('.').append(value);
      }
      value = 0;
      bytes = 0;
    }
    if (bytes != 0) {
      throw new IllegalArgumentException("truncated OID");
    }
    return cap(out.toString());
  }

  private static boolean printable(byte[] b, Element e) {
    for (int i = e.start; i < e.end(); i++) {
      int c = b[i] & 0xFF;
      if ((c < 0x20 || c > 0x7E) && c != '\t' && c != '\r' && c != '\n') {
        return false;
      }
    }
    return true;
  }

  private static String text(byte[] b, Element e) {
    return cap(new String(b, e.start, Math.min(e.length, MAX_STRING), StandardCharsets.UTF_8));
  }

  private static String hex(byte[] b, Element e) {
    StringBuilder out = new StringBuilder();
    for (int i = e.start; i < e.end() && out.length() < MAX_STRING; i++) {
      out.append(String.format("%02x", b[i]));
    }
    return out.toString();
  }

  private static String ipv4(byte[] b, int at) {
    return (b[at] & 0xFF) + "." + (b[at + 1] & 0xFF) + "." + (b[at + 2] & 0xFF) + "." + (b[at + 3] & 0xFF);
  }

  private static String cap(String s) {
    return s.length() > MAX_STRING ? s.substring(0, MAX_STRING) : s;
  }
}
