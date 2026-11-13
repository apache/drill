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
package org.apache.drill.exec.store.pcap.protocol.ftp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestFtpSessionDecoder extends BaseTest {

  static final class Context implements DecoderContext {
    final boolean expose;
    final List<String> warnings = new ArrayList<>();

    Context(boolean expose) {
      this.expose = expose;
    }

    @Override
    public boolean exposeCredentials() {
      return expose;
    }

    @Override
    public void warn(String message) {
      warnings.add(message);
    }
  }

  private static byte[] b(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static final String CLIENT = "USER anonymous\r\nPASS guest@example.com\r\nSYST\r\nPWD\r\nCWD /pub\r\n"
      + "PASV\r\nRETR readme.txt\r\nPORT 10,0,0,1,4,1\r\nSTOR up.bin\r\nEPSV\r\nLIST\r\nQUIT\r\n";
  private static final String SERVER = "220-Welcome\r\n220 FTP ready\r\n331 Password required\r\n230 Logged in\r\n"
      + "215 UNIX Type: L8\r\n257 \"/home/ftp\" is current directory\r\n250 OK\r\n"
      + "227 Entering Passive Mode (10,0,0,2,195,80)\r\n150 Opening\r\n226 Transfer complete\r\n"
      + "200 PORT ok\r\n150 Opening\r\n226 Done\r\n229 Entering Extended Passive Mode (|||6446|)\r\n"
      + "150 Here comes the listing\r\n226 Directory send OK\r\n221 Goodbye\r\n";

  @Test
  public void testLoginAndTransfers() {
    Context context = new Context(false);
    FtpSession s = FtpSessionDecoder.parseStreams(b(CLIENT), b(SERVER), context);
    assertEquals("Welcome\nFTP ready", s.banner);
    assertEquals("anonymous", s.username);
    assertTrue(s.passwordPresent);
    assertNull(s.password);
    assertEquals("UNIX Type: L8", s.systemType);
    assertEquals(Arrays.asList("/home/ftp", "/pub"), s.currentDirectories);
    assertFalse(s.tlsStarted);
    assertEquals(3, s.transfers.size());
    FtpSession.Transfer retr = s.transfers.get(0);
    assertEquals("RETR", retr.command);
    assertEquals("readme.txt", retr.path);
    assertEquals(Integer.valueOf(226), retr.replyCode);
    assertEquals("10.0.0.2:50000", retr.dataAddress);
    assertEquals("10.0.0.1:1025", s.transfers.get(1).dataAddress);
    assertEquals("LIST", s.transfers.get(2).command);
    assertNull(s.transfers.get(2).path);
    assertEquals(":6446", s.transfers.get(2).dataAddress);
    assertEquals(12, s.commands.size());
    assertEquals("***", s.commands.get(1).argument);
    assertEquals(16, s.replies.size());
    assertEquals(220, s.replies.get(0).code);
    assertTrue(context.warnings.isEmpty());
  }

  @Test
  public void testExposedPassword() {
    FtpSession s = FtpSessionDecoder.parseStreams(b(CLIENT), b(SERVER), new Context(true));
    assertEquals("guest@example.com", s.password);
    assertEquals("guest@example.com", s.commands.get(1).argument);
  }

  @Test
  public void testAuthTlsStopsBothDirections() {
    byte[] client = concat(b("AUTH TLS\r\n"), new byte[] {0x16, 0x03, 0x01, 0x00, 0x05, 1, 2, 3, 4, 5});
    byte[] server = concat(b("220 ready\r\n234 Proceed with negotiation.\r\n"), new byte[] {0x16, 0x03, 0x03, 0, 1, 9});
    Context context = new Context(false);
    FtpSession s = FtpSessionDecoder.parseStreams(client, server, context);
    assertTrue(s.tlsStarted);
    assertEquals(1, s.commands.size());
    assertEquals(2, s.replies.size());
    assertTrue(context.warnings.isEmpty());
  }

  @Test
  public void testNotFtp() {
    Context context = new Context(false);
    assertNull(FtpSessionDecoder.parseStreams(b("GET / HTTP/1.1\r\n\r\n"), b("HTTP/1.1 200 OK\r\n\r\n"), context));
    assertNull(FtpSessionDecoder.parseStreams(b("SSH-2.0-x\r\n"), b("SSH-2.0-y\r\n"), context));
    assertNull(FtpSessionDecoder.parseStreams(new byte[0], new byte[0], context));
  }

  @Test
  public void testGarbageAfterCommandsWarns() {
    Context context = new Context(false);
    FtpSession s = FtpSessionDecoder.parseStreams(concat(b("USER bob\r\n"), new byte[] {0, 1, 2, '\r', '\n'}),
        b("220 hi\r\n331 pw\r\n"), context);
    assertEquals("bob", s.username);
    assertEquals(Arrays.asList("unparseable data in client stream at byte 10"), context.warnings);
  }

  @Test
  public void testUnterminatedMultilineReplyWarns() {
    Context context = new Context(false);
    FtpSession s = FtpSessionDecoder.parseStreams(b("USER bob\r\n"), b("220 hi\r\n331-more\r\nstill going\r\n"), context);
    assertEquals(1, s.replies.size());
    assertEquals(Arrays.asList("unterminated multi-line reply in server stream at byte 8"), context.warnings);
  }

  @Test
  public void testMalformedServerStreamThrows() {
    try {
      FtpSessionDecoder.parseStreams(b("USER bob\r\n"), new byte[] {1, 2, 3, 4}, new Context(false));
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("server stream does not start with a reply", e.getMessage());
    }
  }

  @Test
  public void testCapsCommands() {
    StringBuilder client = new StringBuilder("USER u\r\n");
    StringBuilder server = new StringBuilder("220 hi\r\n331 pw\r\n");
    for (int i = 0; i < 70; i++) {
      client.append("NOOP\r\n");
      server.append("200 ok\r\n");
    }
    Context context = new Context(false);
    FtpSession s = FtpSessionDecoder.parseStreams(b(client.toString()), b(server.toString()), context);
    assertEquals(64, s.commands.size());
    assertEquals(64, s.replies.size());
    assertEquals(Arrays.asList("commands truncated to 64", "replies truncated to 64"), context.warnings);
  }

  @Test
  public void testDataAddressParsing() {
    assertEquals("10.0.0.1:1025", FtpSessionDecoder.portAddress("10,0,0,1,4,1"));
    assertNull(FtpSessionDecoder.portAddress("10,0,0,1,4,300"));
    assertNull(FtpSessionDecoder.portAddress("10,0,0,1"));
    assertEquals("132.235.1.2:6275", FtpSessionDecoder.eprtAddress("|1|132.235.1.2|6275|"));
    assertEquals("[1080::8:800:200c:417a]:5282", FtpSessionDecoder.eprtAddress("|2|1080::8:800:200C:417A|5282|"));
    assertNull(FtpSessionDecoder.eprtAddress("|1|x|"));
    assertEquals(":6446", FtpSessionDecoder.epsvAddress("Entering Extended Passive Mode (|||6446|)"));
    assertEquals("10.0.0.2:50000", FtpSessionDecoder.pasvAddress("Entering Passive Mode (10,0,0,2,195,80)."));
  }

  private static byte[] concat(byte[] a, byte[] c) {
    byte[] out = Arrays.copyOf(a, a.length + c.length);
    System.arraycopy(c, 0, out, a.length, c.length);
    return out;
  }
}
