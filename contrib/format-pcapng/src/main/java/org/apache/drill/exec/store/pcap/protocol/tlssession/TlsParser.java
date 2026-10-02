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
package org.apache.drill.exec.store.pcap.protocol.tlssession;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;

/** Parses the bodies of ClientHello, ServerHello and Certificate messages with bounded work. */
final class TlsParser {
  static final int MAX_CERTIFICATES = 10;
  static final int MAX_LIST = 64;
  static final int MAX_STRING = 4096;
  static final int TLS13 = 0x0304;

  /** The ServerHello random value that marks a HelloRetryRequest (RFC 8446 section 4.1.3). */
  static final byte[] HELLO_RETRY_RANDOM = hex(
      "CF21AD74E59A6111BE1D8C021E65B891C2A211167ABB8C5E079E09E2C8A8339C");

  private static final int EXT_SERVER_NAME = 0;
  private static final int EXT_ALPN = 16;
  private static final int EXT_PRE_SHARED_KEY = 41;
  private static final int EXT_SUPPORTED_VERSIONS = 43;

  private static final Map<Integer, String> CIPHER_SUITES = new HashMap<>();

  static {
    String[] suites = {
        "0004", "TLS_RSA_WITH_RC4_128_MD5",
        "0005", "TLS_RSA_WITH_RC4_128_SHA",
        "000A", "TLS_RSA_WITH_3DES_EDE_CBC_SHA",
        "002F", "TLS_RSA_WITH_AES_128_CBC_SHA",
        "0033", "TLS_DHE_RSA_WITH_AES_128_CBC_SHA",
        "0035", "TLS_RSA_WITH_AES_256_CBC_SHA",
        "0039", "TLS_DHE_RSA_WITH_AES_256_CBC_SHA",
        "003C", "TLS_RSA_WITH_AES_128_CBC_SHA256",
        "003D", "TLS_RSA_WITH_AES_256_CBC_SHA256",
        "009C", "TLS_RSA_WITH_AES_128_GCM_SHA256",
        "009D", "TLS_RSA_WITH_AES_256_GCM_SHA384",
        "009E", "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
        "009F", "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384",
        "1301", "TLS_AES_128_GCM_SHA256",
        "1302", "TLS_AES_256_GCM_SHA384",
        "1303", "TLS_CHACHA20_POLY1305_SHA256",
        "1304", "TLS_AES_128_CCM_SHA256",
        "1305", "TLS_AES_128_CCM_8_SHA256",
        "C009", "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
        "C00A", "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
        "C013", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
        "C014", "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
        "C023", "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
        "C024", "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
        "C027", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
        "C028", "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
        "C02B", "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
        "C02C", "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
        "C02F", "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
        "C030", "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
        "CCA8", "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
        "CCA9", "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
        "CCAA", "TLS_DHE_RSA_WITH_CHACHA20_POLY1305_SHA256"
    };
    for (int i = 0; i < suites.length; i += 2) {
      CIPHER_SUITES.put(Integer.parseInt(suites[i], 16), suites[i + 1]);
    }
  }

  private TlsParser() {
  }

  /** Bounds-checked reader over a message body. Every failure is an IllegalArgumentException. */
  private static final class Cursor {
    final byte[] data;
    int position;
    final int end;

    Cursor(byte[] data, int start, int end) {
      this.data = data;
      this.position = start;
      this.end = end;
    }

    void need(int n) {
      if (n > end - position) {
        throw new IllegalArgumentException("needs " + n + " bytes at offset " + position + ", has " + (end - position));
      }
    }

    int u8() {
      need(1);
      return data[position++] & 0xFF;
    }

    int u16() {
      need(2);
      int v = ((data[position] & 0xFF) << 8) | (data[position + 1] & 0xFF);
      position += 2;
      return v;
    }

    int u24() {
      need(3);
      int v = ((data[position] & 0xFF) << 16) | ((data[position + 1] & 0xFF) << 8) | (data[position + 2] & 0xFF);
      position += 3;
      return v;
    }

    byte[] bytes(int n) {
      need(n);
      byte[] b = Arrays.copyOfRange(data, position, position + n);
      position += n;
      return b;
    }

    /** A sub-cursor over the next n bytes, which this cursor skips. */
    Cursor sub(int n) {
      need(n);
      Cursor c = new Cursor(data, position, position + n);
      position += n;
      return c;
    }

    boolean hasMore() {
      return position < end;
    }

    String string(int n) {
      need(n);
      String s = new String(data, position, n, StandardCharsets.UTF_8);
      position += n;
      return cap(s);
    }
  }

  static void parseClientHello(byte[] body, TlsHandshake h, DecoderContext context) {
    Cursor c = new Cursor(body, 0, body.length);
    h.clientVersion = versionName(c.u16());
    c.bytes(32); // random
    h.clientSessionId = sessionId(c);
    int suitesLength = c.u16();
    if (suitesLength % 2 != 0) {
      throw new IllegalArgumentException("odd cipher suites length " + suitesLength);
    }
    c.sub(suitesLength);
    c.sub(c.u8()); // compression methods
    if (c.hasMore()) {
      Cursor extensions = c.sub(c.u16());
      while (extensions.hasMore()) {
        int type = extensions.u16();
        Cursor e = extensions.sub(extensions.u16());
        switch (type) {
          case EXT_SERVER_NAME:
            readServerName(e, h);
            break;
          case EXT_ALPN:
            readProtocols(e.sub(e.u16()), h.alpnOffered, "ALPN protocols", context);
            break;
          case EXT_SUPPORTED_VERSIONS:
            readVersions(e.sub(e.u8()), h.clientSupportedVersions, context);
            break;
          default:
            break;
        }
      }
    }
    h.clientHelloSeen = true;
  }

  /** @return true if this ServerHello is a HelloRetryRequest */
  static boolean parseServerHello(byte[] body, TlsHandshake h) {
    Cursor c = new Cursor(body, 0, body.length);
    int version = c.u16();
    boolean retry = Arrays.equals(c.bytes(32), HELLO_RETRY_RANDOM);
    byte[] sessionId = sessionId(c);
    int suite = c.u16();
    c.u8(); // compression method
    String alpn = null;
    boolean psk = false;
    if (c.hasMore()) {
      Cursor extensions = c.sub(c.u16());
      while (extensions.hasMore()) {
        int type = extensions.u16();
        Cursor e = extensions.sub(extensions.u16());
        switch (type) {
          case EXT_SUPPORTED_VERSIONS:
            version = e.u16();
            break;
          case EXT_ALPN:
            Cursor list = e.sub(e.u16());
            alpn = list.string(list.u8());
            break;
          case EXT_PRE_SHARED_KEY:
            psk = true;
            break;
          default:
            break;
        }
      }
    }
    // Everything is valid: update the handshake
    h.selectedVersion = version;
    h.serverVersion = versionName(version);
    h.serverSessionId = sessionId;
    h.cipherSuite = suite;
    h.cipherSuiteName = cipherSuiteName(suite);
    h.alpnSelected = alpn;
    h.pskAccepted = psk;
    h.helloRetryRequest = retry || Boolean.TRUE.equals(h.helloRetryRequest);
    return retry;
  }

  /** Parses a TLS 1.0 to 1.2 Certificate message: a list of DER certificates, leaf first. */
  static void parseCertificates(byte[] body, TlsHandshake h, DecoderContext context) {
    Cursor c = new Cursor(body, 0, body.length);
    Cursor list = c.sub(c.u24());
    int count = 0;
    while (list.hasMore()) {
      byte[] der = list.bytes(list.u24());
      if (count < MAX_CERTIFICATES) {
        h.certificates.add(TlsCertificate.parse(der, count, context));
      }
      count++;
    }
    if (count > MAX_CERTIFICATES) {
      context.warn("kept the first " + MAX_CERTIFICATES + " of " + count + " certificates");
    }
    h.certificateCount = count;
  }

  private static byte[] sessionId(Cursor c) {
    int length = c.u8();
    if (length > 32) {
      throw new IllegalArgumentException("session id length " + length);
    }
    return c.bytes(length);
  }

  private static void readServerName(Cursor e, TlsHandshake h) {
    if (!e.hasMore()) {
      return; // A ServerHello may echo an empty server_name extension
    }
    Cursor list = e.sub(e.u16());
    while (list.hasMore()) {
      int nameType = list.u8();
      String name = list.string(list.u16());
      if (nameType == 0 && h.sni == null) {
        h.sni = name;
      }
    }
  }

  private static void readProtocols(Cursor list, List<String> out, String what, DecoderContext context) {
    while (list.hasMore()) {
      String p = list.string(list.u8());
      if (out.size() == MAX_LIST) {
        context.warn("kept the first " + MAX_LIST + " " + what);
        return;
      }
      out.add(p);
    }
  }

  private static void readVersions(Cursor list, List<String> out, DecoderContext context) {
    while (list.hasMore()) {
      int v = list.u16();
      if (isGrease(v)) {
        continue;
      }
      if (out.size() == MAX_LIST) {
        context.warn("kept the first " + MAX_LIST + " supported versions");
        return;
      }
      out.add(versionName(v));
    }
  }

  /** GREASE values (RFC 8701) are 0x?A?A with equal bytes; they are noise, not real versions. */
  static boolean isGrease(int v) {
    return (v & 0x0F0F) == 0x0A0A && (v >> 8) == (v & 0xFF);
  }

  static String versionName(int v) {
    switch (v) {
      case 0x0300:
        return "SSL 3.0";
      case 0x0301:
        return "TLS 1.0";
      case 0x0302:
        return "TLS 1.1";
      case 0x0303:
        return "TLS 1.2";
      case TLS13:
        return "TLS 1.3";
      default:
        return (v & 0xFF00) == 0x7F00 ? "TLS 1.3 draft " + (v & 0xFF) : String.format("0x%04x", v);
    }
  }

  static String cipherSuiteName(int suite) {
    String name = CIPHER_SUITES.get(suite);
    return name != null ? name : String.format("0x%04x", suite);
  }

  static String cap(String s) {
    return s.length() > MAX_STRING ? s.substring(0, MAX_STRING) : s;
  }

  private static byte[] hex(String s) {
    byte[] b = new byte[s.length() / 2];
    for (int i = 0; i < b.length; i++) {
      b[i] = (byte) Integer.parseInt(s.substring(2 * i, 2 * i + 2), 16);
    }
    return b;
  }
}
