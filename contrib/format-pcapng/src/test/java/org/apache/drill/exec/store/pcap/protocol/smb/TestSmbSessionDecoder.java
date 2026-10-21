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
package org.apache.drill.exec.store.pcap.protocol.smb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.ntlm.NtlmMessage;
import org.apache.drill.exec.store.pcap.protocol.ntlm.NtlmParser;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestSmbSessionDecoder extends BaseTest {

  private static final int CMD_NEGOTIATE = 0;
  private static final int CMD_SESSION_SETUP = 1;
  private static final int FLAG_RESPONSE = 0x00000001;

  static final class Context implements DecoderContext {
    final List<String> warnings = new ArrayList<>();

    @Override
    public boolean exposeCredentials() {
      return false;
    }

    @Override
    public void warn(String message) {
      warnings.add(message);
    }
  }

  private static byte[] frame(byte[] message) {
    byte[] out = new byte[4 + message.length];
    out[0] = 0;
    out[1] = (byte) ((message.length >> 16) & 0xFF);
    out[2] = (byte) ((message.length >> 8) & 0xFF);
    out[3] = (byte) (message.length & 0xFF);
    System.arraycopy(message, 0, out, 4, message.length);
    return out;
  }

  private static byte[] header(int command, boolean response, int nextCommand) {
    byte[] h = new byte[64];
    h[0] = (byte) 0xFE;
    h[1] = 'S';
    h[2] = 'M';
    h[3] = 'B';
    le16(h, 4, 64);
    le16(h, 12, command);
    le32(h, 16, response ? FLAG_RESPONSE : 0);
    le32(h, 20, nextCommand);
    return h;
  }

  private static byte[] guidBytes(int start) {
    byte[] g = new byte[16];
    for (int i = 0; i < 16; i++) {
      g[i] = (byte) (start + i);
    }
    return g;
  }

  private static byte[] negotiateRequest(int[] dialects, byte[] clientGuid) {
    byte[] body = new byte[36 + dialects.length * 2];
    le16(body, 0, 36);
    le16(body, 2, dialects.length);
    System.arraycopy(clientGuid, 0, body, 12, 16);
    for (int i = 0; i < dialects.length; i++) {
      le16(body, 36 + i * 2, dialects[i]);
    }
    return concat(header(CMD_NEGOTIATE, false, 0), body);
  }

  private static byte[] negotiateResponse(int dialect, boolean signingRequired, byte[] serverGuid) {
    byte[] body = new byte[64];
    le16(body, 0, 65);
    le16(body, 2, signingRequired ? 0x0002 : 0x0001);
    le16(body, 4, dialect);
    System.arraycopy(serverGuid, 0, body, 8, 16);
    return concat(header(CMD_NEGOTIATE, true, 0), body);
  }

  /** Session setup carrying a security blob; the blob is placed right after a 24/8-byte body. */
  private static byte[] sessionSetup(boolean response, byte[] blob) {
    int bodyLen = response ? 8 : 24;
    int secOffset = 64 + bodyLen;
    byte[] body = new byte[bodyLen + blob.length];
    if (response) {
      le16(body, 0, 9);
      le16(body, 4, secOffset);
      le16(body, 6, blob.length);
    } else {
      le16(body, 0, 25);
      le16(body, 12, secOffset);
      le16(body, 14, blob.length);
    }
    System.arraycopy(blob, 0, body, bodyLen, blob.length);
    return concat(header(CMD_SESSION_SETUP, response, 0), body);
  }

  // --- minimal NTLM message builders (identity fields only) ---

  private static byte[] ntlmAuthenticate(String domain, String user, String workstation, int ntLen) {
    ByteArrayOutputStream payload = new ByteArrayOutputStream();
    byte[] header = new byte[64];
    System.arraycopy(NtlmParser.SIGNATURE, 0, header, 0, 8);
    le32(header, 8, NtlmMessage.AUTHENTICATE);
    secbuf2(header, 12, 64, payload, new byte[24]);                  // LM (dummy)
    secbuf2(header, 20, 64, payload, new byte[ntLen]);               // NT: length drives v1/v2
    byte[] d = domain.getBytes(StandardCharsets.UTF_16LE);
    byte[] u = user.getBytes(StandardCharsets.UTF_16LE);
    byte[] w = workstation.getBytes(StandardCharsets.UTF_16LE);
    secbuf2(header, 28, 64, payload, d);
    secbuf2(header, 36, 64, payload, u);
    secbuf2(header, 44, 64, payload, w);
    secbuf2(header, 52, 64, payload, new byte[0]);
    le32(header, 60, 0x00000001);                                   // UNICODE flag
    return concat(header, payload.toByteArray());
  }

  private static byte[] ntlmChallenge() {
    byte[] header = new byte[48];
    System.arraycopy(NtlmParser.SIGNATURE, 0, header, 0, 8);
    le32(header, 8, NtlmMessage.CHALLENGE);
    le32(header, 20, 0x00000001);
    return header;
  }

  /** Writes a security-buffer descriptor whose length is forced to {@code len} but data is {@code value}. */
  private static void secbuf(byte[] header, int at, int len, int base, ByteArrayOutputStream payload, byte[] value) {
    int offset = base + payload.size();
    le16(header, at, len);
    le16(header, at + 2, len);
    le32(header, at + 4, offset);
    payload.write(value, 0, value.length);
  }

  private static void secbuf2(byte[] header, int at, int base, ByteArrayOutputStream payload, byte[] value) {
    secbuf(header, at, value.length, base, payload, value);
  }

  private static byte[] concat(byte[]... parts) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] p : parts) {
      out.write(p, 0, p.length);
    }
    return out.toByteArray();
  }

  private static void le16(byte[] b, int at, int v) {
    b[at] = (byte) (v & 0xFF);
    b[at + 1] = (byte) ((v >> 8) & 0xFF);
  }

  private static void le32(byte[] b, int at, long v) {
    b[at] = (byte) (v & 0xFF);
    b[at + 1] = (byte) ((v >> 8) & 0xFF);
    b[at + 2] = (byte) ((v >> 16) & 0xFF);
    b[at + 3] = (byte) ((v >> 24) & 0xFF);
  }

  @Test
  public void testFullSession() {
    Context ctx = new Context();
    byte[] client = concat(
        frame(negotiateRequest(new int[] {0x0202, 0x0210, 0x0300, 0x0311}, guidBytes(0))),
        frame(sessionSetup(false, ntlmAuthenticate("CORP", "alice", "WS01", 48))));
    byte[] server = concat(
        frame(negotiateResponse(0x0311, true, guidBytes(0x10))),
        frame(sessionSetup(true, ntlmChallenge())));
    SmbSession s = SmbSessionDecoder.parseStreams(client, server, ctx);
    assertEquals("3.1.1", s.dialect);
    assertEquals(Arrays.asList("2.0.2", "2.1.0", "3.0.0", "3.1.1"), s.clientDialects);
    assertTrue(s.signingRequired);
    assertFalse(s.encryption);
    assertEquals("03020100-0504-0706-0809-0a0b0c0d0e0f", s.clientGuid);
    assertEquals("13121110-1514-1716-1819-1a1b1c1d1e1f", s.serverGuid);
    assertEquals("ntlmssp", s.authType);
    assertEquals("CORP", s.domainName);
    assertEquals("alice", s.userName);
    assertEquals("WS01", s.workstation);
    assertEquals("NTLMv2", s.ntlmVersion);
    assertTrue(ctx.warnings.isEmpty());
  }

  @Test
  public void testNtlmV1() {
    Context ctx = new Context();
    byte[] client = frame(sessionSetup(false, ntlmAuthenticate("WG", "bob", "PC", 24)));
    byte[] server = frame(negotiateResponse(0x0300, false, guidBytes(0x20)));
    SmbSession s = SmbSessionDecoder.parseStreams(client, server, ctx);
    assertEquals("3.0.0", s.dialect);
    assertFalse(s.signingRequired);
    assertEquals("bob", s.userName);
    assertEquals("NTLMv1", s.ntlmVersion);
  }

  @Test
  public void testKerberos() {
    Context ctx = new Context();
    byte[] oid = {0x2a, (byte) 0x86, 0x48, (byte) 0x86, (byte) 0xf7, 0x12, 0x01, 0x02, 0x02};
    byte[] blob = concat(new byte[] {0x60, 0x40, 0x06, 0x09}, oid, new byte[32]);
    byte[] client = frame(sessionSetup(false, blob));
    byte[] server = frame(negotiateResponse(0x0311, true, guidBytes(0x10)));
    SmbSession s = SmbSessionDecoder.parseStreams(client, server, ctx);
    assertEquals("kerberos", s.authType);
    assertNull(s.userName);
    assertEquals("3.1.1", s.dialect);
  }

  @Test
  public void testCompounded() {
    Context ctx = new Context();
    // Two compounded SMB2 messages in one frame: negotiate response then session-setup response.
    byte[] neg = negotiateResponse(0x0311, true, guidBytes(0x10));
    byte[] ss = sessionSetup(true, ntlmChallenge());
    byte[] first = neg.clone();
    le32(first, 20, neg.length); // NextCommand points to the second message
    byte[] server = frame(concat(first, ss));
    SmbSession s = SmbSessionDecoder.parseStreams(new byte[0], server, ctx);
    assertEquals("3.1.1", s.dialect);
    assertEquals("ntlmssp", s.authType);
    assertTrue(ctx.warnings.isEmpty());
  }

  @Test
  public void testEncryption() {
    Context ctx = new Context();
    byte[] transform = new byte[52];
    transform[0] = (byte) 0xFD;
    transform[1] = 'S';
    transform[2] = 'M';
    transform[3] = 'B';
    byte[] server = frame(transform);
    SmbSession s = SmbSessionDecoder.parseStreams(new byte[0], server, ctx);
    assertTrue(s.encryption);
  }

  @Test
  public void testSmb1Only() {
    Context ctx = new Context();
    byte[] smb1 = new byte[40];
    smb1[0] = (byte) 0xFF;
    smb1[1] = 'S';
    smb1[2] = 'M';
    smb1[3] = 'B';
    SmbSession s = SmbSessionDecoder.parseStreams(frame(smb1), frame(smb1), ctx);
    assertEquals("SMB1", s.dialect);
    assertNull(s.authType);
  }

  @Test
  public void testNotSmb() {
    Context ctx = new Context();
    assertNull(SmbSessionDecoder.parseStreams("GET / HTTP/1.1\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1),
        "HTTP/1.1 200 OK\r\n".getBytes(StandardCharsets.ISO_8859_1), ctx));
    assertNull(SmbSessionDecoder.parseStreams(new byte[0], new byte[0], ctx));
  }

  @Test
  public void testTruncatedHeaderWarns() {
    Context ctx = new Context();
    // A frame that announces an SMB2 message but is shorter than a 64-byte header.
    byte[] shortMessage = {(byte) 0xFE, 'S', 'M', 'B', 0, 0, 0, 0};
    byte[] client = frame(shortMessage);
    byte[] server = frame(negotiateResponse(0x0311, true, guidBytes(0x10)));
    SmbSession s = SmbSessionDecoder.parseStreams(client, server, ctx);
    assertEquals("3.1.1", s.dialect);
    assertEquals(Arrays.asList("truncated SMB2 header in client stream at byte 4"), ctx.warnings);
  }
}
