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
package org.apache.drill.exec.store.pcap.protocol.tls;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;

/**
 * Parses the TLS ClientHello or ServerHello at the start of a TCP segment (RFC 8446, RFC 5246),
 * and computes the JA3 or JA3S fingerprint of a complete hello.
 */
public final class TlsParser {
  static final int MAX_ITEMS = 64;
  static final String CONTINUES = "handshake continues in a later segment";
  private static final int MAX_STRING = 4096;
  /** Largest TLSCiphertext fragment: 2^14 + 2048. */
  private static final int MAX_RECORD = 18432;
  private static final int CONTENT_HANDSHAKE = 22;
  private static final int CLIENT_HELLO = 1;
  private static final int SERVER_HELLO = 2;
  /** Version, random, session ID length, one cipher suite, one compression method length and method. */
  private static final int MIN_CLIENT_HELLO = 41;
  /** Version, random, session ID length, cipher suite, compression method. */
  private static final int MIN_SERVER_HELLO = 38;
  private static final char[] HEX = "0123456789abcdef".toCharArray();

  /** The data ran out before a field was complete. */
  private static final class End extends RuntimeException {
    End(String message) {
      super(message, null, false, false);
    }
  }

  private final byte[] b;
  private int legacyVersion;
  private int pos;
  private int limit;

  private TlsParser(byte[] b) {
    this.b = b;
  }

  /**
   * @return the hello, or null if the data does not start with a TLS ClientHello or ServerHello
   * @throws IllegalArgumentException if the data is a hello but malformed
   */
  public static TlsHello parse(byte[] data, DecoderContext context) {
    if (data == null || data.length < 6 || data[0] != CONTENT_HANDSHAKE || data[1] != 3 || data[2] < 0
        || data[2] > 4) {
      return null;
    }
    int recordLength = u16(data, 3);
    int type = data[5];
    if (recordLength < 4 || recordLength > MAX_RECORD || (type != CLIENT_HELLO && type != SERVER_HELLO)) {
      return null;
    }
    TlsHello hello = new TlsHello();
    hello.handshakeType = type == CLIENT_HELLO ? "client_hello" : "server_hello";
    hello.recordVersion = versionName(u16(data, 1));
    // Only the first record is read: a hello fragmented over several records ends at this record
    int available = Math.min(data.length, 5 + recordLength);
    if (available < 9) {
      context.warn(CONTINUES);
      return hello;
    }
    int handshakeEnd = 9 + u24(data, 6);
    if (handshakeEnd - 9 < (type == CLIENT_HELLO ? MIN_CLIENT_HELLO : MIN_SERVER_HELLO)) {
      return null;
    }
    if (available >= 11 && (u16(data, 9) < 0x0300 || u16(data, 9) > 0x0304)) {
      return null;
    }
    TlsParser parser = new TlsParser(data);
    parser.pos = 9;
    parser.limit = Math.min(handshakeEnd, available);
    boolean complete;
    try {
      if (type == CLIENT_HELLO) {
        parser.clientHello(hello, handshakeEnd);
      } else {
        parser.serverHello(hello, handshakeEnd);
      }
      complete = true;
    } catch (End e) {
      if (handshakeEnd <= available) {
        throw new IllegalArgumentException("truncated " + hello.handshakeType + ": " + e.getMessage());
      }
      context.warn(CONTINUES);
      complete = false;
    }
    if (complete) {
      fingerprint(hello, parser.legacyVersion, type == CLIENT_HELLO);
    }
    hello.cipherSuites = cap(hello.cipherSuites, "cipher_suites", context);
    cap(hello.extensions, "extensions", context);
    hello.supportedGroups = cap(hello.supportedGroups, "supported_groups", context);
    hello.ecPointFormats = cap(hello.ecPointFormats, "ec_point_formats", context);
    hello.signatureAlgorithms = cap(hello.signatureAlgorithms, "signature_algorithms", context);
    hello.supportedVersions = cap(hello.supportedVersions, "supported_versions", context);
    hello.alpn = cap(hello.alpn, "alpn", context);
    return hello;
  }

  private void clientHello(TlsHello hello, int handshakeEnd) {
    legacyVersion = u16(need(2));
    hello.version = versionName(legacyVersion);
    need(32);
    sessionId(hello);
    int cipherLength = u16(need(2));
    if (cipherLength % 2 != 0) {
      throw new IllegalArgumentException("odd cipher suites length " + cipherLength);
    }
    List<Integer> ciphers = new ArrayList<>();
    hello.cipherSuites = ciphers;
    for (int i = 0; i < cipherLength; i += 2) {
      ciphers.add(u16(need(2)));
    }
    need(u8(need(1)));
    extensions(hello, handshakeEnd, true);
  }

  private void serverHello(TlsHello hello, int handshakeEnd) {
    legacyVersion = u16(need(2));
    hello.version = versionName(legacyVersion);
    need(32);
    sessionId(hello);
    hello.cipherSuite = u16(need(2));
    need(1);
    extensions(hello, handshakeEnd, false);
  }

  private void sessionId(TlsHello hello) {
    int length = u8(need(1));
    if (length > 32) {
      throw new IllegalArgumentException("session ID length " + length);
    }
    int at = need(length);
    hello.sessionId = length == 0 ? null : hex(b, at, length);
  }

  private void extensions(TlsHello hello, int handshakeEnd, boolean client) {
    if (pos == handshakeEnd) {
      return;
    }
    int length = u16(need(2));
    if (pos + length > handshakeEnd) {
      throw new IllegalArgumentException("extensions length " + length + " exceeds the hello");
    }
    int end = pos + length;
    while (pos < end) {
      int type = u16(need(2));
      int extLength = u16(need(2));
      if (pos + extLength > end) {
        throw new IllegalArgumentException("extension " + type + " length " + extLength + " exceeds the extensions");
      }
      int extStart = need(extLength);
      hello.extensions.add(type);
      int savedLimit = limit;
      pos = extStart;
      limit = extStart + extLength;
      try {
        extension(hello, type, client);
      } catch (End e) {
        throw new IllegalArgumentException("bad " + extensionName(type) + " extension: " + e.getMessage());
      }
      pos = extStart + extLength;
      limit = savedLimit;
    }
  }

  private void extension(TlsHello hello, int type, boolean client) {
    switch (type) {
      case 0:
        if (limit > pos) {
          hello.sni = serverName();
        }
        break;
      case 10:
        hello.supportedGroups = u16List(u16(need(2)));
        break;
      case 11: {
        int n = u8(need(1));
        List<Integer> formats = new ArrayList<>();
        for (int i = 0; i < n; i++) {
          formats.add(u8(need(1)));
        }
        hello.ecPointFormats = formats;
        break;
      }
      case 13:
        hello.signatureAlgorithms = u16List(u16(need(2)));
        break;
      case 16: {
        int length = u16(need(2));
        int end = need(length) + length;
        pos = end - length;
        List<String> protocols = new ArrayList<>();
        while (pos < end) {
          int n = u8(need(1));
          if (pos + n > end) {
            throw new End("protocol name of " + n + " bytes exceeds the list");
          }
          protocols.add(text(need(n), n));
        }
        hello.alpn = protocols;
        break;
      }
      case 43: {
        List<String> versions = new ArrayList<>();
        if (client) {
          List<Integer> values = u16List(u8(need(1)));
          for (int v : values) {
            versions.add(versionName(v));
          }
        } else {
          versions.add(versionName(u16(need(2))));
        }
        hello.supportedVersions = versions;
        break;
      }
      default:
        break;
    }
  }

  /** The first host_name of a server_name list. */
  private String serverName() {
    int length = u16(need(2));
    int end = need(length) + length;
    pos = end - length;
    String name = null;
    while (pos < end) {
      int nameType = u8(need(1));
      int n = u16(need(2));
      if (pos + n > end) {
        throw new End("name of " + n + " bytes exceeds the list");
      }
      int at = need(n);
      if (nameType == 0 && name == null) {
        name = text(at, n);
      }
    }
    return name;
  }

  private List<Integer> u16List(int length) {
    if (length % 2 != 0) {
      throw new End("odd list length " + length);
    }
    int at = need(length);
    List<Integer> values = new ArrayList<>();
    for (int i = 0; i < length; i += 2) {
      values.add(u16(at + i));
    }
    return values;
  }

  /** Reserves n bytes, returning their offset. */
  private int need(int n) {
    if (n > limit - pos) {
      throw new End("needs " + n + " bytes at offset " + pos);
    }
    int at = pos;
    pos += n;
    return at;
  }

  private int u8(int at) {
    return b[at] & 0xFF;
  }

  private int u16(int at) {
    return u16(b, at);
  }

  private static int u16(byte[] b, int at) {
    return ((b[at] & 0xFF) << 8) | (b[at + 1] & 0xFF);
  }

  private static int u24(byte[] b, int at) {
    return ((b[at] & 0xFF) << 16) | u16(b, at + 1);
  }

  private String text(int at, int n) {
    return new String(b, at, Math.min(n, MAX_STRING), StandardCharsets.UTF_8);
  }

  static boolean isGrease(int value) {
    return (value & 0x0F0F) == 0x0A0A && (value >> 8) == (value & 0xFF);
  }

  static String versionName(int version) {
    switch (version) {
      case 0x0300:
        return "SSL 3.0";
      case 0x0301:
        return "TLS 1.0";
      case 0x0302:
        return "TLS 1.1";
      case 0x0303:
        return "TLS 1.2";
      case 0x0304:
        return "TLS 1.3";
      default:
        return String.format("0x%04x", version);
    }
  }

  private static String extensionName(int type) {
    switch (type) {
      case 0:
        return "server_name";
      case 10:
        return "supported_groups";
      case 11:
        return "ec_point_formats";
      case 13:
        return "signature_algorithms";
      case 16:
        return "alpn";
      case 43:
        return "supported_versions";
      default:
        return Integer.toString(type);
    }
  }

  /**
   * JA3: SSLVersion,Ciphers,Extensions,EllipticCurves,EllipticCurvePointFormats with GREASE values
   * removed. JA3S: SSLVersion,Cipher,Extensions. Values are decimal; list items are joined with dashes.
   */
  private static void fingerprint(TlsHello hello, int version, boolean client) {
    if (client) {
      hello.ja3 = version + "," + join(hello.cipherSuites, true) + "," + join(hello.extensions, true) + ","
          + join(hello.supportedGroups, true) + "," + join(hello.ecPointFormats, false);
      hello.ja3Hash = md5(hello.ja3);
    } else {
      hello.ja3s = version + "," + hello.cipherSuite + "," + join(hello.extensions, false);
      hello.ja3sHash = md5(hello.ja3s);
    }
  }

  private static String join(List<Integer> values, boolean skipGrease) {
    StringBuilder s = new StringBuilder();
    if (values != null) {
      for (int v : values) {
        if (skipGrease && isGrease(v)) {
          continue;
        }
        if (s.length() > 0) {
          s.append('-');
        }
        s.append(v);
      }
    }
    return s.toString();
  }

  private static String md5(String s) {
    try {
      byte[] digest = MessageDigest.getInstance("MD5").digest(s.getBytes(StandardCharsets.US_ASCII));
      return hex(digest, 0, digest.length);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private static String hex(byte[] b, int at, int n) {
    char[] c = new char[2 * n];
    for (int i = 0; i < n; i++) {
      c[2 * i] = HEX[(b[at + i] >> 4) & 0xF];
      c[2 * i + 1] = HEX[b[at + i] & 0xF];
    }
    return new String(c);
  }

  private static <T> List<T> cap(List<T> values, String name, DecoderContext context) {
    if (values != null && values.size() > MAX_ITEMS) {
      context.warn(name + " truncated to " + MAX_ITEMS);
      values.subList(MAX_ITEMS, values.size()).clear();
    }
    return values;
  }
}
