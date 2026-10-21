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
package org.apache.drill.exec.store.pcap.protocol.quic;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.tls.TlsHello;
import org.apache.drill.exec.store.pcap.protocol.tls.TlsParser;

/**
 * Parses a QUIC long-header packet (RFC 9000) and, for a version 1 Initial (RFC 9001), decrypts it with the
 * Initial keys derived from the Destination Connection ID and the published salt (section 5.2). Those keys use
 * only public values carried in the packet, so the TLS ClientHello inside can be read by passive inspection.
 * No user traffic is decrypted: later packets use keys negotiated by the handshake, which this never sees.
 */
public final class QuicParser {
  /** The version 1 Initial salt, RFC 9001 section 5.2. */
  private static final byte[] INITIAL_SALT_V1 = {
      0x38, 0x76, 0x2c, (byte) 0xf7, (byte) 0xf5, 0x59, 0x34, (byte) 0xb3, 0x4d, 0x17,
      (byte) 0x9a, (byte) 0xe6, (byte) 0xa4, (byte) 0xc8, 0x0c, (byte) 0xad, (byte) 0xcc, (byte) 0xbb,
      0x7f, 0x0a};
  private static final long VERSION_1 = 0x00000001L;
  private static final int MAX_CID = 20;
  private static final int SAMPLE_LENGTH = 16;
  private static final int GCM_TAG_BITS = 128;
  private static final int MAX_CRYPTO = 65536;
  private static final int MAX_FRAMES = 4096;
  private static final String[] PACKET_TYPES = {"initial", "0rtt", "handshake", "retry"};
  private static final char[] HEX = "0123456789abcdef".toCharArray();

  private QuicParser() {
  }

  /**
   * @return null if the payload is not a QUIC long-header packet; a QuicInitial otherwise
   * @throws IllegalArgumentException if the payload is a version 1 Initial but malformed or cannot be decrypted
   */
  public static QuicInitial parse(byte[] p, DecoderContext context) {
    if (p == null || p.length < 6) {
      return null;
    }
    int b0 = p[0] & 0xFF;
    if ((b0 & 0x80) == 0) {
      // Short header (1-RTT): not a handshake packet, nothing to inspect
      return null;
    }
    long version = u32(p, 1);
    int pos = 5;
    int dcidLen = p[pos++] & 0xFF;
    if (dcidLen > MAX_CID || pos + dcidLen > p.length) {
      return null;
    }
    byte[] dcid = Arrays.copyOfRange(p, pos, pos + dcidLen);
    pos += dcidLen;
    if (pos >= p.length) {
      return null;
    }
    int scidLen = p[pos++] & 0xFF;
    if (scidLen > MAX_CID || pos + scidLen > p.length) {
      return null;
    }
    byte[] scid = Arrays.copyOfRange(p, pos, pos + scidLen);
    pos += scidLen;

    QuicInitial q = new QuicInitial();
    q.version = hex(p, 1, 4);
    q.dcid = dcidLen == 0 ? null : hex(dcid, 0, dcidLen);
    q.scid = scidLen == 0 ? null : hex(scid, 0, scidLen);
    if (version == 0) {
      q.packetType = "version_negotiation";
      return q;
    }
    if (version != VERSION_1) {
      // Another QUIC version: the Initial keys and header format may differ, so stop without decrypting
      return q;
    }
    if ((b0 & 0x40) == 0) {
      // The fixed bit is clear: not a valid QUIC version 1 packet
      return null;
    }
    int type = (b0 >> 4) & 0x03;
    q.packetType = PACKET_TYPES[type];
    if (type != 0) {
      // Only the Initial packet is decrypted; 0-RTT, Handshake and Retry are reported but not decrypted
      return q;
    }
    // From here the packet is confidently a QUIC version 1 Initial: problems are reported, not swallowed
    decryptInitial(p, pos, b0, dcid, q, context);
    return q;
  }

  private static void decryptInitial(byte[] p, int pos, int b0, byte[] dcid, QuicInitial q,
                                     DecoderContext context) {
    long[] token = varint(p, pos);
    pos = (int) token[1];
    if (token[0] > p.length - pos) {
      throw new IllegalArgumentException("token length " + token[0] + " exceeds the packet");
    }
    pos += (int) token[0];
    long[] lengthField = varint(p, pos);
    pos = (int) lengthField[1];
    long length = lengthField[0];
    int pnOffset = pos;
    if (length < 1 || length > p.length - pnOffset) {
      throw new IllegalArgumentException("packet length " + length + " exceeds the datagram");
    }
    if (pnOffset + 4 + SAMPLE_LENGTH > p.length) {
      throw new IllegalArgumentException("truncated: no header protection sample");
    }

    byte[] secret = clientInitialSecret(dcid);
    byte[] key = expandLabel(secret, "quic key", 16);
    byte[] iv = expandLabel(secret, "quic iv", 12);
    byte[] headerProtection = expandLabel(secret, "quic hp", 16);

    byte[] sample = Arrays.copyOfRange(p, pnOffset + 4, pnOffset + 4 + SAMPLE_LENGTH);
    byte[] mask = aesEcb(headerProtection, sample);
    int firstByte = b0 ^ (mask[0] & 0x0F);
    int pnLength = (firstByte & 0x03) + 1;
    if (pnLength > length) {
      throw new IllegalArgumentException("packet number length " + pnLength + " exceeds the packet");
    }
    byte[] header = Arrays.copyOfRange(p, 0, pnOffset + pnLength);
    header[0] = (byte) firstByte;
    long packetNumber = 0;
    for (int i = 0; i < pnLength; i++) {
      int value = (p[pnOffset + i] ^ mask[1 + i]) & 0xFF;
      header[pnOffset + i] = (byte) value;
      packetNumber = (packetNumber << 8) | value;
    }

    byte[] nonce = iv.clone();
    for (int i = 0; i < 8; i++) {
      nonce[nonce.length - 1 - i] ^= (packetNumber >>> (8 * i)) & 0xFF;
    }
    byte[] ciphertext = Arrays.copyOfRange(p, pnOffset + pnLength, pnOffset + (int) length);
    byte[] plaintext;
    try {
      plaintext = aesGcmDecrypt(key, nonce, header, ciphertext);
    } catch (GeneralSecurityException e) {
      throw new IllegalArgumentException("AEAD authentication failed");
    }

    byte[] crypto = reassembleCrypto(plaintext, context);
    if (crypto == null) {
      return;
    }
    TlsHello hello = TlsParser.parseHandshake(crypto, 0, true, context);
    if (hello != null) {
      q.sni = hello.sni;
      q.alpn = hello.alpn;
      q.supportedVersions = hello.supportedVersions;
      q.cipherSuites = hello.cipherSuites;
      q.ja4 = hello.ja4;
    }
  }

  /** Reassembles CRYPTO frames of a decrypted Initial into one buffer; other frame types are skipped. */
  private static byte[] reassembleCrypto(byte[] p, DecoderContext context) {
    byte[] buffer = null;
    int end = 0;
    int pos = 0;
    int frames = 0;
    try {
      while (pos < p.length && frames++ < MAX_FRAMES) {
        long[] frameType = varint(p, pos);
        pos = (int) frameType[1];
        long type = frameType[0];
        if (type == 0x00 || type == 0x01) {
          // PADDING or PING: no body
          continue;
        }
        if (type == 0x02 || type == 0x03) {
          pos = skipAck(p, pos, type == 0x03);
          continue;
        }
        if (type != 0x06) {
          // An unexpected frame type: stop and keep whatever CRYPTO was already collected
          break;
        }
        long[] offset = varint(p, pos);
        pos = (int) offset[1];
        long[] len = varint(p, pos);
        pos = (int) len[1];
        if (len[0] > p.length - pos) {
          break;
        }
        int segmentEnd = (int) Math.min((long) MAX_CRYPTO, offset[0] + len[0]);
        if (offset[0] < MAX_CRYPTO && segmentEnd > offset[0]) {
          if (buffer == null) {
            buffer = new byte[MAX_CRYPTO];
          }
          System.arraycopy(p, pos, buffer, (int) offset[0], segmentEnd - (int) offset[0]);
          end = Math.max(end, segmentEnd);
        }
        if (offset[0] + len[0] > MAX_CRYPTO) {
          context.warn("CRYPTO data truncated to " + MAX_CRYPTO + " bytes");
        }
        pos += (int) len[0];
      }
    } catch (IllegalArgumentException e) {
      // A malformed frame in the decrypted payload: keep the CRYPTO bytes gathered so far
    }
    return buffer == null ? null : Arrays.copyOf(buffer, end);
  }

  /** Skips an ACK frame, returning the offset just past it. */
  private static int skipAck(byte[] p, int pos, boolean ecn) {
    pos = (int) varint(p, pos)[1];                 // largest acknowledged
    pos = (int) varint(p, pos)[1];                 // ack delay
    long[] rangeCount = varint(p, pos);
    pos = (int) rangeCount[1];
    pos = (int) varint(p, pos)[1];                 // first ack range
    for (long i = 0; i < rangeCount[0] && i < MAX_FRAMES; i++) {
      pos = (int) varint(p, pos)[1];               // gap
      pos = (int) varint(p, pos)[1];               // ack range length
    }
    if (ecn) {
      pos = (int) varint(p, pos)[1];               // ect0
      pos = (int) varint(p, pos)[1];               // ect1
      pos = (int) varint(p, pos)[1];               // ecn-ce
    }
    return pos;
  }

  private static byte[] clientInitialSecret(byte[] dcid) {
    byte[] initialSecret = hkdfExtract(INITIAL_SALT_V1, dcid);
    return expandLabel(initialSecret, "client in", 32);
  }

  private static byte[] hkdfExtract(byte[] salt, byte[] ikm) {
    return hmacSha256(salt, ikm);
  }

  /** HKDF-Expand-Label with an empty context, RFC 8446 section 7.1. */
  private static byte[] expandLabel(byte[] secret, String label, int length) {
    byte[] fullLabel = ("tls13 " + label).getBytes(StandardCharsets.US_ASCII);
    byte[] info = new byte[2 + 1 + fullLabel.length + 1];
    info[0] = (byte) (length >> 8);
    info[1] = (byte) length;
    info[2] = (byte) fullLabel.length;
    System.arraycopy(fullLabel, 0, info, 3, fullLabel.length);
    info[info.length - 1] = 0;
    return hkdfExpand(secret, info, length);
  }

  private static byte[] hkdfExpand(byte[] prk, byte[] info, int length) {
    byte[] out = new byte[length];
    byte[] t = new byte[0];
    int pos = 0;
    byte counter = 1;
    while (pos < length) {
      byte[] input = new byte[t.length + info.length + 1];
      System.arraycopy(t, 0, input, 0, t.length);
      System.arraycopy(info, 0, input, t.length, info.length);
      input[input.length - 1] = counter;
      t = hmacSha256(prk, input);
      int n = Math.min(t.length, length - pos);
      System.arraycopy(t, 0, out, pos, n);
      pos += n;
      counter++;
    }
    return out;
  }

  private static byte[] hmacSha256(byte[] key, byte[] data) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(key.length == 0 ? new byte[1] : key, "HmacSHA256"));
      return mac.doFinal(data);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException(e);
    }
  }

  private static byte[] aesEcb(byte[] key, byte[] data) {
    try {
      Cipher cipher = Cipher.getInstance("AES/ECB/NoPadding");
      cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"));
      return cipher.doFinal(data);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException(e);
    }
  }

  private static byte[] aesGcmDecrypt(byte[] key, byte[] nonce, byte[] aad, byte[] ciphertext)
      throws GeneralSecurityException {
    Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
    cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(GCM_TAG_BITS, nonce));
    cipher.updateAAD(aad);
    return cipher.doFinal(ciphertext);
  }

  /** Reads a QUIC variable-length integer (RFC 9000 section 16), returning {value, nextPosition}. */
  private static long[] varint(byte[] b, int pos) {
    if (pos < 0 || pos >= b.length) {
      throw new IllegalArgumentException("truncated varint");
    }
    int first = b[pos] & 0xFF;
    int len = 1 << (first >> 6);
    if (pos + len > b.length) {
      throw new IllegalArgumentException("truncated varint");
    }
    long value = first & 0x3F;
    for (int i = 1; i < len; i++) {
      value = (value << 8) | (b[pos + i] & 0xFF);
    }
    return new long[] {value, pos + len};
  }

  private static long u32(byte[] b, int at) {
    return ((long) (b[at] & 0xFF) << 24) | ((b[at + 1] & 0xFF) << 16) | ((b[at + 2] & 0xFF) << 8)
        | (b[at + 3] & 0xFF);
  }

  private static String hex(byte[] b, int at, int n) {
    char[] c = new char[2 * n];
    for (int i = 0; i < n; i++) {
      c[2 * i] = HEX[(b[at + i] >> 4) & 0xF];
      c[2 * i + 1] = HEX[b[at + i] & 0xF];
    }
    return new String(c);
  }
}
