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
package org.apache.drill.exec.store.pcap.protocol.ssh;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestSshSessionDecoder extends BaseTest {

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

  private static final String[] CLIENT_LISTS = {
      "curve25519-sha256,diffie-hellman-group14-sha256,ext-info-c", "ssh-ed25519,rsa-sha2-512",
      "chacha20-poly1305@openssh.com,aes128-ctr", "aes256-ctr", "hmac-sha2-256", "hmac-sha1",
      "none,zlib@openssh.com", "none", "", ""};
  private static final String[] SERVER_LISTS = {
      "curve25519-sha256,kex-strict-s-v00@openssh.com", "rsa-sha2-512,ssh-ed25519",
      "aes128-ctr", "aes256-gcm@openssh.com,aes128-ctr", "hmac-sha2-512", "umac-64@openssh.com",
      "none", "none,zlib@openssh.com", "", ""};

  static byte[] kexinit(String[] lists) {
    ByteArrayOutputStream payload = new ByteArrayOutputStream();
    payload.write(20);
    payload.write(new byte[16], 0, 16);
    for (String l : lists) {
      byte[] b = l.getBytes(StandardCharsets.US_ASCII);
      payload.write(ByteBuffer.allocate(4).putInt(b.length).array(), 0, 4);
      payload.write(b, 0, b.length);
    }
    payload.write(new byte[5], 0, 5); // first_kex_packet_follows, reserved
    byte[] p = payload.toByteArray();
    int padding = 8 - (5 + p.length) % 8 + 8;
    return ByteBuffer.allocate(5 + p.length + padding).putInt(1 + p.length + padding).put((byte) padding).put(p)
        .array();
  }

  private static byte[] concat(byte[]... parts) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] p : parts) {
      out.write(p, 0, p.length);
    }
    return out.toByteArray();
  }

  private static byte[] b(String s) {
    return s.getBytes(StandardCharsets.US_ASCII);
  }

  private static byte[] client() {
    return concat(b("SSH-2.0-OpenSSH_9.6 Ubuntu-3ubuntu13\r\n"), kexinit(CLIENT_LISTS), new byte[] {1, 2, 3});
  }

  private static byte[] server() {
    return concat(b("Welcome\r\nSSH-1.99-dropbear_2022.83\r\n"), kexinit(SERVER_LISTS));
  }

  @Test
  public void testHandshake() {
    Context context = new Context();
    SshSession s = SshSessionDecoder.parseStreams(client(), -1, server(), -1, context);
    assertEquals("2.0", s.client.version);
    assertEquals("OpenSSH_9.6", s.client.software);
    assertEquals("Ubuntu-3ubuntu13", s.client.comments);
    assertEquals("1.99", s.server.version);
    assertEquals("dropbear_2022.83", s.server.software);
    assertNull(s.server.comments);
    assertEquals(Arrays.asList("curve25519-sha256", "diffie-hellman-group14-sha256", "ext-info-c"),
        s.client.kexAlgorithms);
    assertEquals(Arrays.asList("ssh-ed25519", "rsa-sha2-512"), s.client.hostKeyAlgorithms);
    // Client side: client-to-server lists; server side: server-to-client lists
    assertEquals(Arrays.asList("chacha20-poly1305@openssh.com", "aes128-ctr"), s.client.ciphers);
    assertEquals(Arrays.asList("hmac-sha2-256"), s.client.macs);
    assertEquals(Arrays.asList("none", "zlib@openssh.com"), s.client.compression);
    assertEquals(Arrays.asList("aes256-gcm@openssh.com", "aes128-ctr"), s.server.ciphers);
    assertEquals(Arrays.asList("umac-64@openssh.com"), s.server.macs);
    assertEquals(Arrays.asList("none", "zlib@openssh.com"), s.server.compression);
    assertEquals("curve25519-sha256,diffie-hellman-group14-sha256,ext-info-c;chacha20-poly1305@openssh.com,aes128-ctr;"
        + "hmac-sha2-256;none,zlib@openssh.com", s.client.hasshString);
    assertEquals("curve25519-sha256,kex-strict-s-v00@openssh.com;aes256-gcm@openssh.com,aes128-ctr;"
        + "umac-64@openssh.com;none,zlib@openssh.com", s.server.hasshString);
    // md5 of the strings above, computed with Python's hashlib
    assertEquals("61fdba3e86ab199259b4aaff7e069330", s.client.hassh);
    assertEquals("b4e8bc36e235851fcd758af62ad6bbfa", s.server.hassh);
    assertTrue(context.warnings.isEmpty());
  }

  @Test
  public void testNotSsh() {
    Context context = new Context();
    assertNull(SshSessionDecoder.parseStreams(b("GET / HTTP/1.1\r\n\r\n"), -1, b("HTTP/1.1 200 OK\r\n"), -1, context));
    assertNull(SshSessionDecoder.parseStreams(b("SSH-1.5-old\r\n"), -1, b("SSH-1.5-old\r\n"), -1, context));
    assertNull(SshSessionDecoder.parseStreams(new byte[0], -1, new byte[0], -1, context));
    assertTrue(context.warnings.isEmpty());
  }

  @Test
  public void testTruncatedKexinitWarns() {
    Context context = new Context();
    byte[] client = client();
    SshSession s = SshSessionDecoder.parseStreams(Arrays.copyOf(client, 60), -1, server(), -1, context);
    assertEquals("OpenSSH_9.6", s.client.software);
    assertNull(s.client.hassh);
    assertTrue(s.server.hassh != null);
    assertEquals(Arrays.asList("truncated KEXINIT in client stream"), context.warnings);
  }

  @Test
  public void testGapWarns() {
    Context context = new Context();
    SshSession s = SshSessionDecoder.parseStreams(Arrays.copyOf(client(), 60), 60, server(), -1, context);
    assertNull(s.client.hassh);
    assertEquals(Arrays.asList("stopped at missing data in client stream at byte 60"), context.warnings);
  }

  @Test
  public void testNameListOverrunWarns() {
    byte[] packet = kexinit(CLIENT_LISTS);
    // First name-list length (after length, padding, type, cookie) claims more than the packet
    ByteBuffer.wrap(packet).putInt(4 + 1 + 1 + 16, 100000);
    Context context = new Context();
    SshSession s = SshSessionDecoder.parseStreams(concat(b("SSH-2.0-x\r\n"), packet), -1, new byte[0], -1, context);
    assertEquals("x", s.client.software);
    assertNull(s.client.kexAlgorithms);
    assertEquals(Arrays.asList("malformed KEXINIT in client stream: name-list 1 overruns the packet"),
        context.warnings);
  }

  @Test
  public void testNotKexinitWarns() {
    byte[] packet = kexinit(CLIENT_LISTS);
    packet[5] = 21;
    Context context = new Context();
    SshSessionDecoder.parseStreams(concat(b("SSH-2.0-x\r\n"), packet), -1, new byte[0], -1, context);
    assertEquals(Arrays.asList("first packet in client stream is type 21, not KEXINIT"), context.warnings);
  }

  @Test
  public void testOverlongIdentificationThrows() {
    char[] longSoftware = new char[300];
    Arrays.fill(longSoftware, 'a');
    try {
      SshSessionDecoder.parseStreams(b("SSH-2.0-" + new String(longSoftware) + "\r\n"), -1, new byte[0], -1,
          new Context());
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("client identification string longer than 255 bytes", e.getMessage());
    }
  }
}
