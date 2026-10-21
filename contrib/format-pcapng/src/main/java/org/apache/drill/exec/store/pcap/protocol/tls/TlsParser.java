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
import java.util.Collections;
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
    return parseBody(data, type, hello, 9, handshakeEnd, available, false, context);
  }

  /**
   * Parses a bare TLS handshake message (ClientHello or ServerHello) that is not wrapped in a TLS record,
   * as carried in QUIC CRYPTO frames. The message starts at {@code offset} with the one-byte handshake
   * type. Computes the JA4 fingerprint with the QUIC transport ('q') when {@code quic} is true.
   *
   * @return the hello, or null if the bytes are not a ClientHello/ServerHello
   * @throws IllegalArgumentException if the bytes are a hello but malformed
   */
  public static TlsHello parseHandshake(byte[] data, int offset, boolean quic, DecoderContext context) {
    if (data == null || offset < 0 || data.length - offset < 4) {
      return null;
    }
    int type = data[offset] & 0xFF;
    if (type != CLIENT_HELLO && type != SERVER_HELLO) {
      return null;
    }
    int bodyStart = offset + 4;
    int handshakeEnd = bodyStart + u24(data, offset + 1);
    if (handshakeEnd - bodyStart < (type == CLIENT_HELLO ? MIN_CLIENT_HELLO : MIN_SERVER_HELLO)) {
      return null;
    }
    int available = data.length;
    if (available >= bodyStart + 2 && (u16(data, bodyStart) < 0x0300 || u16(data, bodyStart) > 0x0304)) {
      return null;
    }
    TlsHello hello = new TlsHello();
    hello.handshakeType = type == CLIENT_HELLO ? "client_hello" : "server_hello";
    return parseBody(data, type, hello, bodyStart, handshakeEnd, available, quic, context);
  }

  private static TlsHello parseBody(byte[] data, int type, TlsHello hello, int bodyStart, int handshakeEnd,
                                    int available, boolean quic, DecoderContext context) {
    TlsParser parser = new TlsParser(data);
    parser.pos = bodyStart;
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
      if (type == CLIENT_HELLO) {
        ja4(hello, parser.legacyVersion, quic);
      }
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
          hello.rawSupportedVersions = values;
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

  /**
   * JA4 TLS client fingerprint (FoxIO, BSD 3-Clause). Format {@code q d c _ a _ b}:
   * {@code a} = transport (t for TCP, q for QUIC), 2-char TLS version (highest supported_versions, else
   * the legacy version), SNI indicator (d present, i absent), 2-digit cipher count and 2-digit extension
   * count (both excluding GREASE, the extension count still counting SNI and ALPN), and the first and last
   * character of the first ALPN value. {@code b} = first 12 hex of SHA-256 of the sorted non-GREASE cipher
   * suites in hex. {@code c} = first 12 hex of SHA-256 of the sorted non-GREASE extensions in hex, with SNI
   * (0x0000) and ALPN (0x0010) removed, then an underscore and the signature algorithms in their original
   * order. Only JA4 is implemented; the JA4S/JA4H/JA4SSH variants have an incompatible licence.
   */
  private static void ja4(TlsHello hello, int legacyVersion, boolean quic) {
    int version = ja4HighestVersion(hello, legacyVersion);
    int cipherCount = Math.min(99, countNonGrease(hello.cipherSuites));
    int extCount = Math.min(99, countNonGrease(hello.extensions));
    String a = (quic ? "q" : "t") + ja4VersionCode(version) + (hello.sni != null ? "d" : "i")
        + twoDigit(cipherCount) + twoDigit(extCount) + ja4Alpn(hello.alpn);

    List<String> cipherHex = new ArrayList<>();
    if (hello.cipherSuites != null) {
      for (int c : hello.cipherSuites) {
        if (!isGrease(c)) {
          cipherHex.add(hex4(c));
        }
      }
    }
    Collections.sort(cipherHex);
    String bRaw = String.join(",", cipherHex);
    String b = cipherHex.isEmpty() ? "000000000000" : sha256Prefix(bRaw);

    List<String> extHex = new ArrayList<>();
    for (int e : hello.extensions) {
      if (!isGrease(e) && e != 0x0000 && e != 0x0010) {
        extHex.add(hex4(e));
      }
    }
    Collections.sort(extHex);
    List<String> sigHex = new ArrayList<>();
    if (hello.signatureAlgorithms != null) {
      for (int s : hello.signatureAlgorithms) {
        if (!isGrease(s)) {
          sigHex.add(hex4(s));
        }
      }
    }
    String cRaw = String.join(",", extHex) + (sigHex.isEmpty() ? "" : "_" + String.join(",", sigHex));
    String c = extHex.isEmpty() ? "000000000000" : sha256Prefix(cRaw);

    hello.ja4 = a + "_" + b + "_" + c;
    hello.ja4Raw = a + "_" + bRaw + "_" + cRaw;
  }

  private static int ja4HighestVersion(TlsHello hello, int legacyVersion) {
    int best = -1;
    if (hello.rawSupportedVersions != null) {
      for (int v : hello.rawSupportedVersions) {
        if (!isGrease(v) && v > best) {
          best = v;
        }
      }
    }
    return best >= 0 ? best : legacyVersion;
  }

  private static String ja4VersionCode(int version) {
    switch (version) {
      case 0x0304:
        return "13";
      case 0x0303:
        return "12";
      case 0x0302:
        return "11";
      case 0x0301:
        return "10";
      case 0x0300:
        return "s3";
      case 0xfefd:
        return "d2";
      case 0xfefc:
        return "d3";
      default:
        return "00";
    }
  }

  private static String ja4Alpn(List<String> alpn) {
    if (alpn == null || alpn.isEmpty() || alpn.get(0).isEmpty()) {
      return "00";
    }
    byte[] value = alpn.get(0).getBytes(StandardCharsets.UTF_8);
    char first = (char) (value[0] & 0xFF);
    char last = (char) (value[value.length - 1] & 0xFF);
    if (isAlphanumeric(first) && isAlphanumeric(last)) {
      return "" + first + last;
    }
    return "" + HEX[(value[0] >> 4) & 0xF] + HEX[value[value.length - 1] & 0xF];
  }

  private static boolean isAlphanumeric(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
  }

  private static int countNonGrease(List<Integer> values) {
    int count = 0;
    if (values != null) {
      for (int v : values) {
        if (!isGrease(v)) {
          count++;
        }
      }
    }
    return count;
  }

  private static String twoDigit(int n) {
    return n < 10 ? "0" + n : Integer.toString(n);
  }

  private static String hex4(int v) {
    return new String(new char[] {HEX[(v >> 12) & 0xF], HEX[(v >> 8) & 0xF], HEX[(v >> 4) & 0xF], HEX[v & 0xF]});
  }

  private static String sha256Prefix(String s) {
    try {
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.US_ASCII));
      return hex(digest, 0, 6);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
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
