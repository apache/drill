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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestNtlmParser extends BaseTest {

  private static final long UNICODE = 0x00000001L;

  /** Builds an NTLMSSP message body with the given 4-byte-aligned fixed header and trailing payload. */
  private static final class Builder {
    private final ByteArrayOutputStream header = new ByteArrayOutputStream();
    private final ByteArrayOutputStream payload = new ByteArrayOutputStream();
    private int headerLen;

    Builder(int type, int headerLen) {
      this.headerLen = headerLen;
      write(header, NtlmParser.SIGNATURE);
      u32(header, type);
    }

    /** Appends an 8-byte security-buffer descriptor whose data is {@code value}, placed in the payload. */
    void secbuf(byte[] value) {
      int offset = headerLen + payload.size();
      u16(header, value.length);
      u16(header, value.length);
      u32(header, offset);
      write(payload, value);
    }

    void u32Field(long v) {
      u32(header, v);
    }

    void raw(int bytes) {
      header.write(new byte[bytes], 0, bytes);
    }

    byte[] build() {
      byte[] h = header.toByteArray();
      byte[] p = payload.toByteArray();
      byte[] out = new byte[h.length + p.length];
      System.arraycopy(h, 0, out, 0, h.length);
      System.arraycopy(p, 0, out, h.length, p.length);
      return out;
    }

    private static void write(ByteArrayOutputStream out, byte[] b) {
      out.write(b, 0, b.length);
    }

    private static void u16(ByteArrayOutputStream out, int v) {
      out.write(v & 0xFF);
      out.write((v >> 8) & 0xFF);
    }

    private static void u32(ByteArrayOutputStream out, long v) {
      out.write((int) (v & 0xFF));
      out.write((int) ((v >> 8) & 0xFF));
      out.write((int) ((v >> 16) & 0xFF));
      out.write((int) ((v >> 24) & 0xFF));
    }
  }

  private static byte[] unicode(String s) {
    return s.getBytes(StandardCharsets.UTF_16LE);
  }

  private static byte[] avPair(int id, byte[] value) {
    byte[] out = new byte[4 + value.length];
    out[0] = (byte) (id & 0xFF);
    out[1] = (byte) ((id >> 8) & 0xFF);
    out[2] = (byte) (value.length & 0xFF);
    out[3] = (byte) ((value.length >> 8) & 0xFF);
    System.arraycopy(value, 0, out, 4, value.length);
    return out;
  }

  @Test
  public void testNegotiate() {
    Builder b = new Builder(NtlmMessage.NEGOTIATE, 16);
    b.u32Field(UNICODE | 0x02000000L);
    NtlmMessage m = NtlmParser.parse(b.build(), 0);
    assertEquals(NtlmMessage.NEGOTIATE, m.messageType);
    assertEquals(UNICODE | 0x02000000L, m.flags);
  }

  @Test
  public void testAuthenticateV2Unicode() {
    // Header: sig(8) type(4) Lm(8) Nt(8) Domain(8) User(8) Workstation(8) SessionKey(8) Flags(4) = 64
    Builder b = new Builder(NtlmMessage.AUTHENTICATE, 64);
    b.secbuf(new byte[24]);         // LM response placeholder
    b.secbuf(new byte[48]);         // NT response: > 24 bytes => NTLMv2
    b.secbuf(unicode("CORP"));      // Domain
    b.secbuf(unicode("alice"));     // User
    b.secbuf(unicode("WS01"));      // Workstation
    b.secbuf(new byte[16]);         // session key
    b.u32Field(UNICODE);
    NtlmMessage m = NtlmParser.parse(b.build(), 0);
    assertEquals(NtlmMessage.AUTHENTICATE, m.messageType);
    assertEquals("CORP", m.domainName);
    assertEquals("alice", m.userName);
    assertEquals("WS01", m.workstation);
    assertEquals("NTLMv2", m.ntlmVersion);
  }

  @Test
  public void testAuthenticateV1Oem() {
    Builder b = new Builder(NtlmMessage.AUTHENTICATE, 64);
    b.secbuf(new byte[24]);
    b.secbuf(new byte[24]);                               // NT response exactly 24 bytes => NTLMv1
    b.secbuf("WORKGROUP".getBytes(StandardCharsets.ISO_8859_1));
    b.secbuf("bob".getBytes(StandardCharsets.ISO_8859_1));
    b.secbuf("PC".getBytes(StandardCharsets.ISO_8859_1));
    b.secbuf(new byte[0]);
    b.u32Field(0L);                                       // no UNICODE flag => OEM strings
    NtlmMessage m = NtlmParser.parse(b.build(), 0);
    assertEquals("WORKGROUP", m.domainName);
    assertEquals("bob", m.userName);
    assertEquals("PC", m.workstation);
    assertEquals("NTLMv1", m.ntlmVersion);
  }

  @Test
  public void testChallengeTargetInfo() {
    // Header: sig(8) type(4) TargetName(8) Flags(4) ServerChallenge(8) Reserved(8) TargetInfo(8) = 48
    Builder b = new Builder(NtlmMessage.CHALLENGE, 48);
    b.secbuf(unicode("CORP"));       // TargetName
    b.u32Field(UNICODE);             // NegotiateFlags
    b.raw(16);                       // ServerChallenge(8) + Reserved(8)
    ByteArrayOutputStream info = new ByteArrayOutputStream();
    byte[] nb = avPair(1, unicode("FILE01"));
    byte[] nbd = avPair(2, unicode("CORP"));
    byte[] dns = avPair(3, unicode("file01.corp.example"));
    byte[] dnsd = avPair(4, unicode("corp.example"));
    byte[] eol = avPair(0, new byte[0]);
    info.write(nb, 0, nb.length);
    info.write(nbd, 0, nbd.length);
    info.write(dns, 0, dns.length);
    info.write(dnsd, 0, dnsd.length);
    info.write(eol, 0, eol.length);
    b.secbuf(info.toByteArray());    // TargetInfo
    NtlmMessage m = NtlmParser.parse(b.build(), 0);
    assertEquals(NtlmMessage.CHALLENGE, m.messageType);
    assertEquals("CORP", m.targetName);
    assertEquals("FILE01", m.targetNetbiosComputer);
    assertEquals("CORP", m.targetNetbiosDomain);
    assertEquals("file01.corp.example", m.targetDnsComputer);
    assertEquals("corp.example", m.targetDnsDomain);
  }

  @Test
  public void testFindSignatureInsideBlob() {
    byte[] prefix = {1, 2, 3, 4, 5};
    Builder b = new Builder(NtlmMessage.NEGOTIATE, 16);
    b.u32Field(UNICODE);
    byte[] msg = b.build();
    byte[] blob = new byte[prefix.length + msg.length];
    System.arraycopy(prefix, 0, blob, 0, prefix.length);
    System.arraycopy(msg, 0, blob, prefix.length, msg.length);
    int at = NtlmParser.findSignature(blob, 0, blob.length);
    assertEquals(prefix.length, at);
    assertEquals(NtlmMessage.NEGOTIATE, NtlmParser.parse(blob, at).messageType);
  }

  @Test
  public void testNotNtlm() {
    assertNull(NtlmParser.parse("not ntlm at all".getBytes(StandardCharsets.ISO_8859_1), 0));
    assertNull(NtlmParser.parse(new byte[4], 0));
  }

  @Test
  public void testOutOfBoundsSecurityBufferRejected() {
    Builder b = new Builder(NtlmMessage.AUTHENTICATE, 64);
    b.secbuf(new byte[24]);          // LM
    b.secbuf(new byte[48]);          // NT
    b.secbuf(unicode("CORP"));       // Domain
    b.secbuf(unicode("alice"));      // User
    b.secbuf(unicode("WS01"));       // Workstation
    b.secbuf(new byte[16]);          // session key
    b.u32Field(UNICODE);
    byte[] msg = b.build();
    // Corrupt the NT security buffer length (descriptor at offset 20) to overrun the message.
    msg[20] = (byte) 0xFF;
    msg[21] = (byte) 0xFF;
    assertNull(NtlmParser.parse(msg, 0));
  }
}
