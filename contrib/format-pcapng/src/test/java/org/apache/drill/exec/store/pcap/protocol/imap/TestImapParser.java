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
package org.apache.drill.exec.store.pcap.protocol.imap;

import static org.apache.drill.exec.store.pcap.protocol.mail.MailTestContext.lines;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

import org.apache.drill.exec.store.pcap.protocol.mail.MailMessage;
import org.apache.drill.exec.store.pcap.protocol.mail.MailTestContext;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestImapParser extends BaseTest {

  static final String HEADER = "From: Carol <carol@example.org>\r\nSubject: =?utf-8?B?SMOpbGxv?=\r\n"
      + "Message-ID: <i2@example.org>\r\n\r\n";

  static final byte[] SERVER = bytes(
      "* OK [CAPABILITY IMAP4rev1 STARTTLS AUTH=PLAIN] Dovecot ready.\r\n"
      + "* CAPABILITY IMAP4rev1 IDLE SORT\r\n"
      + "a1 OK Capability completed.\r\n"
      + "a2 OK Logged in\r\n"
      + "* 2 EXISTS\r\n"
      + "* FLAGS (\\Seen \\Deleted)\r\n"
      + "a3 OK [READ-WRITE] Select completed.\r\n"
      + "a4 NO Mailbox doesn't exist: Junk\r\n"
      + "* 1 FETCH (UID 7 RFC822.SIZE 1234 ENVELOPE (\"Tue, 2 Jan 2024 03:04:05 +0000\" \"Hi there\" "
      + "((\"Alice\" NIL \"alice\" \"example.org\")) ((\"Alice\" NIL \"alice\" \"example.org\")) NIL "
      + "((NIL NIL \"bob\" \"example.com\")(\"=?utf-8?Q?J=C3=B6rg?=\" NIL \"jorg\" \"example.com\")) NIL NIL NIL "
      + "\"<i1@example.org>\"))\r\n"
      + "* 2 FETCH (UID 8 BODY[HEADER] {" + HEADER.length() + "}\r\n" + HEADER + " FLAGS (\\Seen))\r\n"
      + "* 2 FETCH (FLAGS (\\Seen))\r\n"
      + "a5 OK Fetch completed (0.001 + 0.000 secs).\r\n"
      + "* BYE Logging out\r\n"
      + "a6 OK Logout completed.\r\n");

  static final byte[] CLIENT = lines(
      "a1 CAPABILITY",
      "a2 LOGIN bob \"hunter 2\"",
      "a3 SELECT INBOX",
      "a4 EXAMINE Junk",
      "a5 UID FETCH 1:* (UID RFC822.SIZE ENVELOPE BODY.PEEK[HEADER])",
      "a6 LOGOUT");

  static byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  @Test
  public void testFullSession() {
    MailTestContext context = new MailTestContext(false);
    ImapSession s = ImapParser.parse(CLIENT, SERVER, context);
    assertEquals("[CAPABILITY IMAP4rev1 STARTTLS AUTH=PLAIN] Dovecot ready.", s.banner);
    assertEquals(Arrays.asList("IMAP4rev1", "IDLE", "SORT"), s.capabilities.items());
    assertEquals("bob", s.credentials.username);
    assertTrue(s.credentials.passwordPresent);
    assertNull(s.credentials.password);
    assertFalse(s.tlsStarted);
    assertEquals(Arrays.asList("INBOX"), s.selectedMailboxes.items());
    assertEquals(6, s.commands.items().size());
    assertEquals(Arrays.asList("a2", "LOGIN", "bob ***"), Arrays.asList(s.commands.items().get(1)));
    assertEquals(Arrays.asList("a5", "UID", "FETCH 1:* (UID RFC822.SIZE ENVELOPE BODY.PEEK[HEADER])"),
        Arrays.asList(s.commands.items().get(4)));
    assertEquals(6, s.responses.items().size());
    assertEquals(Arrays.asList("a4", "NO", "Mailbox doesn't exist: Junk"), Arrays.asList(s.responses.items().get(3)));
    assertEquals(2, s.fetched.items().size());
    MailMessage m = s.fetched.items().get(0);
    assertEquals(Integer.valueOf(1), m.number);
    assertEquals(Long.valueOf(7), m.uid);
    assertEquals(Long.valueOf(1234), m.size);
    assertEquals("Alice <alice@example.org>", m.headers.from);
    assertEquals("bob@example.com, Jörg <jorg@example.com>", m.headers.to);
    assertEquals("Hi there", m.headers.subject);
    assertEquals("Tue, 2 Jan 2024 03:04:05 +0000", m.headers.date);
    assertEquals("<i1@example.org>", m.headers.messageId);
    m = s.fetched.items().get(1);
    assertEquals(Integer.valueOf(2), m.number);
    assertEquals("Carol <carol@example.org>", m.headers.from);
    assertEquals("Héllo", m.headers.subject);
    assertEquals("<i2@example.org>", m.headers.messageId);
    assertTrue(context.warnings.toString(), context.warnings.isEmpty());
  }

  @Test
  public void testLoginWithLiteralsAndExpose() {
    byte[] client = bytes("a1 LOGIN {3}\r\nbob {7}\r\nhunter2\r\na2 LOGOUT\r\n");
    byte[] server = lines("* OK hi", "+ go", "+ go", "a1 OK done", "a2 OK bye");
    ImapSession s = ImapParser.parse(client, server, new MailTestContext(true));
    assertEquals("bob", s.credentials.username);
    assertEquals("hunter2", s.credentials.password);
    assertEquals(2, s.commands.items().size());
    assertEquals("{3} {7}", s.commands.items().get(0)[2]);
    s = ImapParser.parse(client, server, new MailTestContext(false));
    assertEquals("{3} ***", s.commands.items().get(0)[2]);
  }

  @Test
  public void testAuthenticatePlain() {
    String ir = Base64.getEncoder().encodeToString("\0joe\0pw".getBytes(StandardCharsets.UTF_8));
    byte[] client = lines("a1 AUTHENTICATE PLAIN", ir, "a2 IDLE", "DONE", "a3 LOGOUT");
    byte[] server = lines("* OK hi", "+ ", "a1 OK authenticated", "+ idling", "a2 OK done", "a3 OK bye");
    MailTestContext context = new MailTestContext(false);
    ImapSession s = ImapParser.parse(client, server, context);
    assertEquals("PLAIN", s.credentials.mechanism);
    assertEquals("joe", s.credentials.username);
    assertTrue(s.credentials.passwordPresent);
    assertNull(s.credentials.password);
    assertEquals(3, s.commands.items().size());
    assertEquals("IDLE", s.commands.items().get(1)[1]);
    assertTrue(context.warnings.toString(), context.warnings.isEmpty());
  }

  @Test
  public void testStartTls() {
    byte[] client = TestImapParser.concat(lines("a1 STARTTLS"), new byte[] {0x16, 0x03, 0x01, ' ', '\n'});
    byte[] server = TestImapParser.concat(lines("* OK hi", "a1 OK Begin TLS negotiation now."),
        new byte[] {0x16, 0x03, 0x03, ' ', '\n'});
    MailTestContext context = new MailTestContext(false);
    ImapSession s = ImapParser.parse(client, server, context);
    assertTrue(s.tlsStarted);
    assertEquals(1, s.commands.items().size());
    assertEquals(1, s.responses.items().size());
    assertTrue(context.warnings.toString(), context.warnings.isEmpty());
  }

  @Test
  public void testRefusedStartTlsContinues() {
    byte[] client = lines("a1 STARTTLS", "a2 LOGIN u p");
    byte[] server = lines("* OK hi", "a1 BAD no", "a2 OK in");
    ImapSession s = ImapParser.parse(client, server, new MailTestContext(false));
    assertFalse(s.tlsStarted);
    assertEquals("u", s.credentials.username);
    assertEquals(2, s.responses.items().size());
  }

  @Test
  public void testNotImap() {
    MailTestContext context = new MailTestContext(false);
    assertNull(ImapParser.parse(lines("SSH-2.0-x"), lines("SSH-2.0-y"), context));
    assertNull(ImapParser.parse(new byte[0], new byte[0], context));
    assertNull(ImapParser.parse(lines("hello there"), lines("+OK pop3"), context));
    assertTrue(context.warnings.isEmpty());
  }

  @Test
  public void testLiteralBeyondStreamWarns() {
    MailTestContext context = new MailTestContext(false);
    ImapSession s = ImapParser.parse(lines("a1 FETCH 1 BODY[HEADER]"),
        bytes("* OK hi\r\n* 1 FETCH (BODY[HEADER] {99999}\r\nFrom: x\r\n"), context);
    assertEquals("hi", s.banner);
    assertTrue(s.fetched.items().isEmpty());
    assertEquals(Arrays.asList("literal of 99999 bytes at byte 9 exceeds the server stream"), context.warnings);
  }

  @Test
  public void testHugeLiteralLengthWarns() {
    MailTestContext context = new MailTestContext(false);
    ImapParser.parse(bytes("a1 LOGIN {99999999999999999999}\r\n"), lines("* OK hi"), context);
    assertEquals(Arrays.asList("unparseable command at byte 0 of client stream"), context.warnings);
  }

  @Test
  public void testDeepNestingWarns() {
    StringBuilder b = new StringBuilder("* OK hi\r\n* 1 FETCH ");
    for (int i = 0; i < 100; i++) {
      b.append('(');
    }
    MailTestContext context = new MailTestContext(false);
    ImapParser.parse(lines("a1 NOOP"), bytes(b + "\r\n"), context);
    assertEquals(Arrays.asList("unparseable response at byte 9 of server stream"), context.warnings);
  }

  static byte[] concat(byte[] a, byte[] b) {
    byte[] out = Arrays.copyOf(a, a.length + b.length);
    System.arraycopy(b, 0, out, a.length, b.length);
    return out;
  }
}
