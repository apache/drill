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
package org.apache.drill.exec.store.pcap.protocol.stun;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;

/** Parses STUN messages (RFC 5389, RFC 8489) with bounded work. */
public final class StunParser {
  static final int MAX_ITEMS = 64;
  static final int MAGIC_COOKIE = 0x2112A442;
  private static final int MAX_STRING = 4096;
  private static final String[] CLASSES = {"request", "indication", "success_response", "error_response"};
  private static final String[] METHODS = {null, "binding", null, "allocate", "refresh", null, "send", "data",
      "create_permission", "channel_bind"};
  private static final char[] HEX = "0123456789abcdef".toCharArray();

  private StunParser() { }

  /**
   * @return the message, or null if the data is not STUN
   * @throws IllegalArgumentException if the data is STUN but malformed
   */
  public static StunMessage parse(byte[] b, DecoderContext context) {
    if (b == null || b.length < 20 || (b[0] & 0xC0) != 0) {
      return null;
    }
    int length = u16(b, 2);
    if (length % 4 != 0 || 20 + length > b.length || u32(b, 4) != MAGIC_COOKIE) {
      return null;
    }
    int type = u16(b, 0);
    StunMessage m = new StunMessage();
    m.messageClass = CLASSES[((type >> 7) & 0x2) | ((type >> 4) & 0x1)];
    int method = (type & 0xF) | ((type >> 1) & 0x70) | ((type >> 2) & 0xF80);
    m.messageMethod = method < METHODS.length && METHODS[method] != null ? METHODS[method] : Integer.toString(method);
    StringBuilder id = new StringBuilder();
    for (int i = 8; i < 20; i++) {
      id.append(HEX[(b[i] >> 4) & 0xF]).append(HEX[b[i] & 0xF]);
    }
    m.transactionId = id.toString();
    int end = 20 + length;
    int pos = 20;
    int count = 0;
    // The message length is a multiple of 4, as is every padded attribute, so each step advances
    // by at least 4 and an attribute header always fits
    while (pos < end) {
      int attrType = u16(b, pos);
      int attrLength = u16(b, pos + 2);
      pos += 4;
      if (attrLength > end - pos) {
        throw new IllegalArgumentException(String.format("attribute 0x%04x length %d exceeds the message",
            attrType, attrLength));
      }
      if (count == MAX_ITEMS) {
        context.warn("attributes truncated to " + MAX_ITEMS);
      } else if (count < MAX_ITEMS) {
        m.attributes.add(new int[] {attrType, attrLength});
      }
      count++;
      attribute(m, b, attrType, pos, attrLength);
      pos += (attrLength + 3) & ~3;
    }
    return m;
  }

  private static void attribute(StunMessage m, byte[] b, int type, int at, int length) {
    switch (type) {
      case 0x0001:
        if (m.mappedAddress == null) {
          m.mappedAddress = address(b, at, length, false, "MAPPED-ADDRESS");
        }
        break;
      case 0x0020:
        if (m.xorMappedAddress == null) {
          m.xorMappedAddress = address(b, at, length, true, "XOR-MAPPED-ADDRESS");
        }
        break;
      case 0x0006:
        m.username = m.username == null ? text(b, at, length) : m.username;
        break;
      case 0x0009:
        if (length < 4) {
          throw new IllegalArgumentException("ERROR-CODE length " + length);
        }
        if (m.errorCode == null) {
          m.errorCode = (b[at + 2] & 0x7) * 100 + (b[at + 3] & 0xFF);
          m.errorReason = length > 4 ? text(b, at + 4, length - 4) : null;
        }
        break;
      case 0x0014:
        m.realm = m.realm == null ? text(b, at, length) : m.realm;
        break;
      case 0x0015:
        m.nonce = m.nonce == null ? text(b, at, length) : m.nonce;
        break;
      case 0x8022:
        m.software = m.software == null ? text(b, at, length) : m.software;
        break;
      default:
        break;
    }
  }

  /** ip:port, or [ipv6]:port. An XOR address is XORed with the magic cookie and transaction ID. */
  private static String address(byte[] b, int at, int length, boolean xor, String name) {
    int family = length >= 4 ? b[at + 1] & 0xFF : 0;
    int size = family == 1 ? 4 : family == 2 ? 16 : 0;
    if (size == 0 || length != 4 + size) {
      throw new IllegalArgumentException("bad " + name + ": family " + family + ", length " + length);
    }
    int port = u16(b, at + 2);
    byte[] address = new byte[size];
    System.arraycopy(b, at + 4, address, 0, size);
    if (xor) {
      port ^= MAGIC_COOKIE >>> 16;
      // The cookie and transaction ID are the 16 bytes at offset 4 of the message
      for (int i = 0; i < size; i++) {
        address[i] ^= b[4 + i];
      }
    }
    try {
      String host = InetAddress.getByAddress(address).getHostAddress();
      return (size == 16 ? "[" + host + "]" : host) + ":" + port;
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private static String text(byte[] b, int at, int length) {
    return new String(b, at, Math.min(length, MAX_STRING), StandardCharsets.UTF_8);
  }

  private static int u16(byte[] b, int at) {
    return ((b[at] & 0xFF) << 8) | (b[at + 1] & 0xFF);
  }

  private static int u32(byte[] b, int at) {
    return (u16(b, at) << 16) | u16(b, at + 2);
  }
}
