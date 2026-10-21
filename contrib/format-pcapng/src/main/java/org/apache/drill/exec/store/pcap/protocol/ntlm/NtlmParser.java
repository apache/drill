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
package org.apache.drill.exec.store.pcap.protocol.ntlm;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * A bounded parser for the three NTLMSSP message types (MS-NLMP), used by the SMB session decoder
 * and available to any other decoder that finds an {@code NTLMSSP\0} blob. It is a shared helper, not
 * a registered decoder.
 *
 * <p>It reports only identity metadata: the message type, the negotiate flags, the challenge's target
 * and target-info names, and the authenticate message's user, domain and workstation with an NTLMv1 or
 * NTLMv2 classification. Server challenges, LM/NT responses, session keys and anything derived from them
 * are never read out, so no crackable material ever leaves this class, regardless of exposeCredentials.
 * Every security-buffer offset and length is checked against the bytes available.</p>
 */
public final class NtlmParser {

  /** The 8-byte signature that starts every NTLMSSP message: {@code "NTLMSSP\0"}. */
  public static final byte[] SIGNATURE = {'N', 'T', 'L', 'M', 'S', 'S', 'P', 0};

  private static final long NEGOTIATE_UNICODE = 0x00000001L;

  // AV_PAIR identifiers in the CHALLENGE target info (MS-NLMP 2.2.2.1)
  private static final int AV_EOL = 0x0000;
  private static final int AV_NB_COMPUTER = 0x0001;
  private static final int AV_NB_DOMAIN = 0x0002;
  private static final int AV_DNS_COMPUTER = 0x0003;
  private static final int AV_DNS_DOMAIN = 0x0004;

  private static final int MAX_STRING = 4096;
  private static final int MAX_AV_PAIRS = 64;

  private NtlmParser() {
  }

  /**
   * Finds the first {@code NTLMSSP\0} signature inside {@code data} between {@code from} (inclusive)
   * and {@code to} (exclusive).
   *
   * @return the offset of the signature, or -1 if it is not present
   */
  public static int findSignature(byte[] data, int from, int to) {
    int limit = Math.min(to, data.length) - SIGNATURE.length;
    for (int i = Math.max(0, from); i <= limit; i++) {
      boolean match = true;
      for (int j = 0; j < SIGNATURE.length; j++) {
        if (data[i + j] != SIGNATURE[j]) {
          match = false;
          break;
        }
      }
      if (match) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Parses one NTLMSSP message that begins at {@code start} and extends to the end of {@code data}.
   *
   * @return the parsed identity metadata, or null if the bytes are not a recognizable NTLMSSP message
   *         of a known type (bad signature, impossible type, or any out-of-bounds security buffer)
   */
  public static NtlmMessage parse(byte[] data, int start) {
    return parse(data, start, data.length - start);
  }

  /**
   * Parses one NTLMSSP message occupying {@code [start, start + length)}.
   *
   * @return the parsed identity metadata, or null if the bytes are not a recognizable NTLMSSP message
   */
  public static NtlmMessage parse(byte[] data, int start, int length) {
    if (start < 0 || length < 12 || start + length > data.length) {
      return null;
    }
    for (int i = 0; i < SIGNATURE.length; i++) {
      if (data[start + i] != SIGNATURE[i]) {
        return null;
      }
    }
    int type = (int) u32(data, start + 8);
    switch (type) {
      case NtlmMessage.NEGOTIATE:
        return negotiate(data, start, length);
      case NtlmMessage.CHALLENGE:
        return challenge(data, start, length);
      case NtlmMessage.AUTHENTICATE:
        return authenticate(data, start, length);
      default:
        return null;
    }
  }

  private static NtlmMessage negotiate(byte[] data, int start, int length) {
    if (length < 16) {
      return null;
    }
    NtlmMessage m = new NtlmMessage();
    m.messageType = NtlmMessage.NEGOTIATE;
    m.flags = u32(data, start + 12);
    return m;
  }

  private static NtlmMessage challenge(byte[] data, int start, int length) {
    // sig(8) type(4) TargetNameFields(8) NegotiateFlags(4) ServerChallenge(8) Reserved(8) TargetInfoFields(8)
    if (length < 48) {
      return null;
    }
    long flags = u32(data, start + 20);
    Charset charset = charset(flags);
    NtlmMessage m = new NtlmMessage();
    m.messageType = NtlmMessage.CHALLENGE;
    m.flags = flags;
    m.targetName = securityBufferString(data, start, length, start + 12, charset);
    // The 8-byte server challenge at start+24 is deliberately NOT read: it is crackable material.
    int[] info = securityBuffer(data, start, length, start + 40);
    if (info != null) {
      readTargetInfo(data, info[0], info[1], m);
    }
    return m;
  }

  private static NtlmMessage authenticate(byte[] data, int start, int length) {
    // sig(8) type(4) Lm(8) Nt(8) Domain(8) User(8) Workstation(8) SessionKey(8) NegotiateFlags(4)
    if (length < 64) {
      return null;
    }
    long flags = u32(data, start + 60);
    Charset charset = charset(flags);
    int[] nt = securityBuffer(data, start, length, start + 20);
    if (securityBuffer(data, start, length, start + 12) == null || nt == null) {
      return null;
    }
    NtlmMessage m = new NtlmMessage();
    m.messageType = NtlmMessage.AUTHENTICATE;
    m.flags = flags;
    m.domainName = securityBufferString(data, start, length, start + 28, charset);
    m.userName = securityBufferString(data, start, length, start + 36, charset);
    m.workstation = securityBufferString(data, start, length, start + 44, charset);
    // Only the NT response LENGTH is used, to classify v1 vs v2; the response bytes are never read out.
    if (nt[1] == 24) {
      m.ntlmVersion = "NTLMv1";
    } else if (nt[1] > 24) {
      m.ntlmVersion = "NTLMv2";
    }
    return m;
  }

  private static void readTargetInfo(byte[] data, int offset, int length, NtlmMessage m) {
    int p = offset;
    int end = offset + length;
    for (int pairs = 0; pairs < MAX_AV_PAIRS && p + 4 <= end; pairs++) {
      int avId = u16(data, p);
      int avLen = u16(data, p + 2);
      p += 4;
      if (avId == AV_EOL) {
        break;
      }
      if (avLen < 0 || p + avLen > end) {
        break;
      }
      String value = string(data, p, avLen, StandardCharsets.UTF_16LE);
      switch (avId) {
        case AV_NB_COMPUTER:
          m.targetNetbiosComputer = value;
          break;
        case AV_NB_DOMAIN:
          m.targetNetbiosDomain = value;
          break;
        case AV_DNS_COMPUTER:
          m.targetDnsComputer = value;
          break;
        case AV_DNS_DOMAIN:
          m.targetDnsDomain = value;
          break;
        default:
          break;
      }
      p += avLen;
    }
  }

  private static Charset charset(long flags) {
    return (flags & NEGOTIATE_UNICODE) != 0 ? StandardCharsets.UTF_16LE : StandardCharsets.ISO_8859_1;
  }

  /**
   * Reads a security buffer descriptor (len, maxLen, offset) at {@code at} and returns the absolute
   * offset and length of the data it points to, or null if the buffer falls outside the message.
   */
  private static int[] securityBuffer(byte[] data, int start, int length, int at) {
    int len = u16(data, at);
    long rel = u32(data, at + 4);
    if (rel < 0 || len < 0 || rel + len > length) {
      return null;
    }
    int abs = start + (int) rel;
    if (abs + len > data.length) {
      return null;
    }
    return new int[] {abs, len};
  }

  private static String securityBufferString(byte[] data, int start, int length, int at, Charset charset) {
    int[] buf = securityBuffer(data, start, length, at);
    if (buf == null || buf[1] == 0) {
      return null;
    }
    return string(data, buf[0], buf[1], charset);
  }

  private static String string(byte[] data, int offset, int len, Charset charset) {
    int capped = Math.min(len, MAX_STRING * 2);
    String s = new String(data, offset, capped, charset);
    return s.length() > MAX_STRING ? s.substring(0, MAX_STRING) : s;
  }

  private static int u16(byte[] b, int at) {
    return (b[at] & 0xFF) | ((b[at + 1] & 0xFF) << 8);
  }

  private static long u32(byte[] b, int at) {
    return (b[at] & 0xFFL) | ((b[at + 1] & 0xFFL) << 8) | ((b[at + 2] & 0xFFL) << 16) | ((b[at + 3] & 0xFFL) << 24);
  }
}
