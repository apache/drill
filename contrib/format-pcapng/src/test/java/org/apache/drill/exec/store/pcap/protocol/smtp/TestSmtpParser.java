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
package org.apache.drill.exec.store.pcap.protocol.smtp;

import static org.apache.drill.exec.store.pcap.protocol.mail.MailTestContext.lines;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

import org.apache.drill.exec.store.pcap.protocol.mail.MailTestContext;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestSmtpParser extends BaseTest {

  static String b64(String s) {
    return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
  }

  static final byte[] SERVER = lines(
      "220 mail.example.com ESMTP Postfix",
      "250-mail.example.com",
      "250-PIPELINING",
      "250-SIZE 10240000",
      "250 AUTH PLAIN LOGIN",
      "235 2.7.0 Authentication successful",
      "250 2.1.0 Ok",
      "250 2.1.5 Ok",
      "250 2.1.5 Ok",
      "354 End data with <CR><LF>.<CR><LF>",
      "250 2.0.0 Ok: queued as 12345",
      "221 2.0.0 Bye");

  static final byte[] CLIENT = lines(
      "EHLO client.example.org",
      "AUTH PLAIN " + b64("\0alice\0secret"),
      "MAIL FROM:<alice@example.org> SIZE=100",
      "RCPT TO:<bob@example.com>",
      "RCPT TO:<carol@example.com>",
      "DATA",
      "From: Alice <alice@example.org>",
      "To: bob@example.com",
      "Cc: carol@example.com",
      "Subject: =?utf-8?Q?Caf=C3=A9?=",
      "Date: Tue, 2 Jan 2024 03:04:05 +0000",
      "Message-ID: <m1@example.org>",
      "",
      "..leading dot",
      "body",
      ".",
      "QUIT");

  @Test
  public void testFullSession() {
    MailTestContext context = new MailTestContext(false);
    SmtpSession s = SmtpParser.parse(CLIENT, SERVER, context);
    assertEquals("mail.example.com ESMTP Postfix", s.banner);
    assertEquals("client.example.org", s.helo);
    assertEquals(Arrays.asList("PIPELINING", "SIZE 10240000", "AUTH PLAIN LOGIN"), s.extensions.items());
    assertEquals("PLAIN", s.credentials.mechanism);
    assertEquals("alice", s.credentials.username);
    assertTrue(s.credentials.passwordPresent);
    assertNull(s.credentials.password);
    assertFalse(s.tlsStarted);
    assertEquals("alice@example.org", s.mailFrom);
    assertEquals(Arrays.asList("bob@example.com", "carol@example.com"), s.rcptTo.items());
    assertEquals(1, s.messages.items().size());
    assertEquals(1, s.messageCount);
    assertEquals("Alice <alice@example.org>", s.messages.items().get(0).headers.from);
    assertEquals("Café", s.messages.items().get(0).headers.subject);
    assertEquals("<m1@example.org>", s.messages.items().get(0).headers.messageId);
    String body = "From: Alice <alice@example.org>\r\nTo: bob@example.com\r\nCc: carol@example.com\r\n"
        + "Subject: =?utf-8?Q?Caf=C3=A9?=\r\nDate: Tue, 2 Jan 2024 03:04:05 +0000\r\n"
        + "Message-ID: <m1@example.org>\r\n\r\n.leading dot\r\nbody\r\n";
    assertEquals(Long.valueOf(body.length()), s.messages.items().get(0).size);
    assertEquals(7, s.commands.items().size());
    assertEquals("AUTH", s.commands.items().get(1)[0]);
    assertEquals("PLAIN ***", s.commands.items().get(1)[1]);
    assertEquals("QUIT", s.commands.items().get(6)[0]);
    assertEquals("", s.commands.items().get(6)[1]);
    assertEquals(12 - 3, s.replies.items().size());
    assertEquals(Integer.valueOf(220), s.replies.items().get(0).code);
    assertEquals("mail.example.com\nPIPELINING\nSIZE 10240000\nAUTH PLAIN LOGIN", s.replies.items().get(1).text);
    assertEquals(Integer.valueOf(221), s.replies.items().get(8).code);
    assertTrue(context.warnings.toString(), context.warnings.isEmpty());
  }

  @Test
  public void testExposeCredentials() {
    SmtpSession s = SmtpParser.parse(CLIENT, SERVER, new MailTestContext(true));
    assertEquals("secret", s.credentials.password);
    assertEquals("PLAIN " + b64("\0alice\0secret"), s.commands.items().get(1)[1]);
  }

  @Test
  public void testAuthLogin() {
    byte[] server = lines("220 hi", "250 hi", "334 VXNlcm5hbWU6", "334 UGFzc3dvcmQ6", "235 ok", "221 bye");
    byte[] client = lines("HELO me", "AUTH LOGIN", b64("bob"), b64("hunter2"), "QUIT");
    SmtpSession s = SmtpParser.parse(client, server, new MailTestContext(true));
    assertEquals("me", s.helo);
    assertEquals("LOGIN", s.credentials.mechanism);
    assertEquals("bob", s.credentials.username);
    assertEquals("hunter2", s.credentials.password);
    // Continuation lines are not commands
    assertEquals(3, s.commands.items().size());
    assertEquals("QUIT", s.commands.items().get(2)[0]);
    assertTrue(s.extensions.items().isEmpty());
  }

  @Test
  public void testCramMd5HasNoPassword() {
    byte[] server = lines("220 hi", "334 PDEyMzQ+", "235 ok");
    byte[] client = lines("AUTH CRAM-MD5", b64("joe 0123456789abcdef"));
    SmtpSession s = SmtpParser.parse(client, server, new MailTestContext(true));
    assertEquals("joe", s.credentials.username);
    assertTrue(s.credentials.passwordPresent);
    assertNull(s.credentials.password);
  }

  @Test
  public void testStartTlsStopsParsing() {
    byte[] server = concat(lines("220 hi", "250-hi", "250 STARTTLS", "220 2.0.0 Ready to start TLS"),
        new byte[] {0x16, 0x03, 0x03, 0x00, 0x10, '\r', '\n'});
    byte[] client = concat(lines("EHLO me", "STARTTLS"), new byte[] {0x16, 0x03, 0x01, 0x02, 0x00, '\n'});
    MailTestContext context = new MailTestContext(false);
    SmtpSession s = SmtpParser.parse(client, server, context);
    assertTrue(s.tlsStarted);
    assertEquals(Arrays.asList("STARTTLS"), s.extensions.items());
    assertEquals(2, s.commands.items().size());
    assertEquals(3, s.replies.items().size());
    assertTrue(context.warnings.toString(), context.warnings.isEmpty());
  }

  @Test
  public void testNotSmtp() {
    MailTestContext context = new MailTestContext(false);
    assertNull(SmtpParser.parse(lines("SSH-2.0-OpenSSH_9.6"), lines("SSH-2.0-OpenSSH_9.6"), context));
    assertNull(SmtpParser.parse(new byte[0], new byte[0], context));
    assertNull(SmtpParser.parse(lines("GET / HTTP/1.1"), lines("HTTP/1.1 200 OK"), context));
    // 465 style implicit TLS bytes
    assertNull(SmtpParser.parse(new byte[] {0x16, 0x03, 0x01, '\n'}, new byte[0], context));
    assertTrue(context.warnings.isEmpty());
  }

  @Test
  public void testTruncatedData() {
    byte[] server = lines("220 hi", "250 ok", "250 ok", "250 ok", "354 go");
    byte[] client = lines("HELO me", "MAIL FROM:<a@b>", "RCPT TO:<c@d>", "DATA", "Subject: cut", "", "part");
    MailTestContext context = new MailTestContext(false);
    SmtpSession s = SmtpParser.parse(client, server, context);
    assertEquals(1, s.messages.items().size());
    assertEquals("cut", s.messages.items().get(0).headers.subject);
    assertEquals(Arrays.asList("message data truncated at byte 47"), context.warnings);
  }

  @Test
  public void testGarbageCommandWarns() {
    byte[] server = lines("220 hi", "500 what");
    byte[] client = concat(lines("HELO me"), new byte[] {1, 2, 3, ' ', '\r', '\n'});
    MailTestContext context = new MailTestContext(false);
    SmtpSession s = SmtpParser.parse(client, server, context);
    assertEquals(1, s.commands.items().size());
    assertEquals(Arrays.asList("unparseable command at byte 9 of client stream"), context.warnings);
  }

  @Test
  public void testMalformedGreetingThrows() {
    try {
      SmtpParser.parse(lines("EHLO me"), lines("hello there"), new MailTestContext(false));
      throw new AssertionError("expected exception");
    } catch (IllegalArgumentException e) {
      assertEquals("malformed reply at byte 0 of server stream", e.getMessage());
    }
  }

  @Test
  public void testBdat() {
    byte[] server = lines("220 hi", "250 ok", "250 ok", "250 ok", "250 ok", "250 ok");
    byte[] client = concat(lines("EHLO me", "MAIL FROM:<a@b>", "RCPT TO:<c@d>", "BDAT 14"),
        "Subject: x\r\n\r\n".getBytes(StandardCharsets.UTF_8), lines("BDAT 3 LAST"),
        "abc".getBytes(StandardCharsets.UTF_8));
    MailTestContext context = new MailTestContext(false);
    SmtpSession s = SmtpParser.parse(client, server, context);
    assertEquals(1, s.messages.items().size());
    assertEquals("x", s.messages.items().get(0).headers.subject);
    assertEquals(Long.valueOf(17), s.messages.items().get(0).size);
    assertTrue(context.warnings.toString(), context.warnings.isEmpty());
  }

  @Test
  public void testCommandsAreCapped() {
    StringBuilder c = new StringBuilder();
    for (int i = 0; i < 70; i++) {
      c.append("NOOP\r\n");
    }
    MailTestContext context = new MailTestContext(false);
    SmtpSession s = SmtpParser.parse(c.toString().getBytes(StandardCharsets.UTF_8), lines("220 hi"), context);
    assertEquals(64, s.commands.items().size());
    assertEquals(70, s.commandCount);
    assertEquals(Arrays.asList("commands capped at 64"), context.warnings);
  }

  static byte[] concat(byte[]... parts) {
    int n = 0;
    for (byte[] p : parts) {
      n += p.length;
    }
    byte[] out = new byte[n];
    int o = 0;
    for (byte[] p : parts) {
      System.arraycopy(p, 0, out, o, p.length);
      o += p.length;
    }
    return out;
  }
}
