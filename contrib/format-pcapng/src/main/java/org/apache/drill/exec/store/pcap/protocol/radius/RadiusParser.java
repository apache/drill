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
package org.apache.drill.exec.store.pcap.protocol.radius;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;

/** Parses RADIUS packets with bounded work. Encrypted attribute values are never decrypted. */
public final class RadiusParser {
  static final int MAX_ITEMS = 64;
  private static final int MAX_STRING = 4096;
  private static final int HEADER = 20;
  private static final int MAX_LENGTH = 4096;
  private static final Map<Integer, String> CODES = new HashMap<>();
  private static final Map<Integer, String> ACCT_STATUS = new HashMap<>();

  static {
    Object[][] codes = {{1, "Access-Request"}, {2, "Access-Accept"}, {3, "Access-Reject"},
        {4, "Accounting-Request"}, {5, "Accounting-Response"}, {11, "Access-Challenge"}, {12, "Status-Server"},
        {13, "Status-Client"}, {40, "Disconnect-Request"}, {41, "Disconnect-ACK"}, {42, "Disconnect-NAK"},
        {43, "CoA-Request"}, {44, "CoA-ACK"}, {45, "CoA-NAK"}};
    for (Object[] c : codes) {
      CODES.put((Integer) c[0], (String) c[1]);
    }
    Object[][] status = {{1, "Start"}, {2, "Stop"}, {3, "Interim-Update"}, {7, "Accounting-On"},
        {8, "Accounting-Off"}, {15, "Failed"}};
    for (Object[] s : status) {
      ACCT_STATUS.put((Integer) s[0], (String) s[1]);
    }
  }

  private RadiusParser() { }

  /**
   * @return the packet, or null if the data is not RADIUS
   * @throws IllegalArgumentException if the data is RADIUS but malformed or truncated
   */
  public static RadiusMessage parse(byte[] b, DecoderContext context) {
    if (b.length < HEADER) {
      return null;
    }
    String codeName = CODES.get(b[0] & 0xFF);
    int length = u16(b, 2);
    if (codeName == null || length < HEADER || length > MAX_LENGTH || length < b.length) {
      return null;
    }
    if (length > b.length) {
      // Only call it truncated RADIUS if the attributes that are present are well formed
      int p = HEADER;
      while (p + 2 <= b.length) {
        int len = b[p + 1] & 0xFF;
        if (len < 2) {
          return null;
        }
        p += len;
      }
      throw new IllegalArgumentException("truncated: length " + length + " but " + b.length + " bytes");
    }
    RadiusMessage m = new RadiusMessage();
    m.code = b[0] & 0xFF;
    m.codeName = codeName;
    m.identifier = b[1] & 0xFF;
    m.authenticator = hex(b, 4, 16);
    int p = HEADER;
    int index = 0;
    while (p < length) {
      index++;
      if (p + 2 > length) {
        throw new IllegalArgumentException("attribute " + index + " truncated at offset " + p);
      }
      int type = b[p] & 0xFF;
      int len = b[p + 1] & 0xFF;
      if (len < 2 || p + len > length) {
        throw new IllegalArgumentException("attribute " + index + " has bad length " + len + " at offset " + p);
      }
      attribute(m, type, b, p + 2, len - 2, context);
      if (index <= MAX_ITEMS) {
        RadiusMessage.Attribute a = new RadiusMessage.Attribute();
        a.type = type;
        boolean secret = type == 2 || type == 3 || type == 69;
        a.value = secret && !context.exposeCredentials() ? null : hex(b, p + 2, len - 2);
        m.attributes.add(a);
      } else if (index == MAX_ITEMS + 1) {
        context.warn("attributes truncated to " + MAX_ITEMS);
      }
      p += len;
    }
    return m;
  }

  /** Fills the named fields; the first occurrence of each attribute wins. */
  private static void attribute(RadiusMessage m, int type, byte[] b, int at, int len, DecoderContext context) {
    switch (type) {
      case 1:
        m.username = m.username != null ? m.username : text(b, at, len);
        break;
      case 2:
      case 3:
        m.passwordPresent = true;
        break;
      case 4:
        m.nasIpAddress = m.nasIpAddress != null ? m.nasIpAddress : address(b, at, len, "NAS-IP-Address", context);
        break;
      case 5:
        if (m.nasPort == null) {
          m.nasPort = len == 4 ? u32(b, at) : null;
          if (len != 4) {
            context.warn("NAS-Port has length " + len);
          }
        }
        break;
      case 8:
        m.framedIpAddress = m.framedIpAddress != null ? m.framedIpAddress
            : address(b, at, len, "Framed-IP-Address", context);
        break;
      case 18:
        m.replyMessage = m.replyMessage != null ? m.replyMessage : text(b, at, len);
        break;
      case 30:
        m.calledStationId = m.calledStationId != null ? m.calledStationId : text(b, at, len);
        break;
      case 31:
        m.callingStationId = m.callingStationId != null ? m.callingStationId : text(b, at, len);
        break;
      case 32:
        m.nasIdentifier = m.nasIdentifier != null ? m.nasIdentifier : text(b, at, len);
        break;
      case 40:
        if (m.acctStatusType == null) {
          if (len == 4) {
            long v = u32(b, at);
            String name = ACCT_STATUS.get((int) v);
            m.acctStatusType = name != null ? name : String.valueOf(v);
          } else {
            context.warn("Acct-Status-Type has length " + len);
          }
        }
        break;
      case 44:
        m.acctSessionId = m.acctSessionId != null ? m.acctSessionId : text(b, at, len);
        break;
      default:
        break;
    }
  }

  private static String address(byte[] b, int at, int len, String name, DecoderContext context) {
    if (len != 4) {
      context.warn(name + " has length " + len);
      return null;
    }
    return (b[at] & 0xFF) + "." + (b[at + 1] & 0xFF) + "." + (b[at + 2] & 0xFF) + "." + (b[at + 3] & 0xFF);
  }

  private static String text(byte[] b, int at, int len) {
    return new String(b, at, len, StandardCharsets.UTF_8);
  }

  private static String hex(byte[] b, int at, int len) {
    StringBuilder out = new StringBuilder();
    for (int i = at; i < at + len && out.length() < MAX_STRING; i++) {
      out.append(String.format("%02x", b[i]));
    }
    return out.toString();
  }

  private static int u16(byte[] b, int at) {
    return ((b[at] & 0xFF) << 8) | (b[at + 1] & 0xFF);
  }

  private static long u32(byte[] b, int at) {
    return ((long) u16(b, at) << 16) | u16(b, at + 2);
  }
}
