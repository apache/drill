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
package org.apache.drill.exec.store.pcap.protocol.pop3;

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

public class TestPop3Parser extends BaseTest {

  static final byte[] SERVER = lines(
      "+OK POP3 server ready <1896.697170952@dbc.mtview.ca.us>",
      "+OK",
      "USER",
      "UIDL",
      ".",
      "+OK send PASS",
      "+OK logged in",
      "+OK 2 320",
      "+OK 2 messages",
      "1 120",
      "2 200",
      ".",
      "+OK 120 octets",
      "From: Alice <alice@example.org>",
      "To: bob@example.com",
      "Subject: Hi",
      "Message-ID: <p1@example.org>",
      "",
      "..dot",
      ".",
      "-ERR no such message",
      "+OK bye");

  static final byte[] CLIENT = lines("CAPA", "USER bob", "PASS hunter2", "STAT", "LIST", "RETR 1", "RETR 9", "QUIT");

  @Test
  public void testFullSession() {
    MailTestContext context = new MailTestContext(false);
    Pop3Session s = Pop3Parser.parse(CLIENT, SERVER, context);
    assertEquals("POP3 server ready <1896.697170952@dbc.mtview.ca.us>", s.banner);
    assertEquals(Arrays.asList("USER", "UIDL"), s.capabilities.items());
    assertEquals("bob", s.credentials.username);
    assertTrue(s.credentials.passwordPresent);
    assertNull(s.credentials.password);
    assertFalse(s.tlsStarted);
    assertEquals(Integer.valueOf(2), s.messageCount);
    assertEquals(1, s.retrieved.items().size());
    MailMessage m = s.retrieved.items().get(0);
    assertEquals(Integer.valueOf(1), m.number);
    assertEquals("Alice <alice@example.org>", m.headers.from);
    assertEquals("Hi", m.headers.subject);
    String body = "From: Alice <alice@example.org>\r\nTo: bob@example.com\r\nSubject: Hi\r\n"
        + "Message-ID: <p1@example.org>\r\n\r\n.dot\r\n";
    assertEquals(Long.valueOf(body.length()), m.size);
    assertEquals(8, s.commands.items().size());
    assertEquals("PASS", s.commands.items().get(2)[0]);
    assertEquals("***", s.commands.items().get(2)[1]);
    assertEquals(9, s.replies.items().size());
    assertEquals("-ERR", s.replies.items().get(7)[0]);
    assertEquals("no such message", s.replies.items().get(7)[1]);
    assertTrue(context.warnings.toString(), context.warnings.isEmpty());
  }

  @Test
  public void testExposeCredentials() {
    Pop3Session s = Pop3Parser.parse(CLIENT, SERVER, new MailTestContext(true));
    assertEquals("hunter2", s.credentials.password);
    assertEquals("hunter2", s.commands.items().get(2)[1]);
  }

  @Test
  public void testApop() {
    Pop3Session s = Pop3Parser.parse(lines("APOP mrose c4c9334bac560ecc979e58001b3e22fb"),
        lines("+OK ready <1@x>", "+OK maildrop has 1 message"), new MailTestContext(true));
    assertEquals("mrose", s.credentials.username);
    assertEquals("APOP", s.credentials.mechanism);
    assertTrue(s.credentials.passwordPresent);
    assertNull(s.credentials.password);
    // The digest is shown only when credentials are exposed
    assertEquals("mrose c4c9334bac560ecc979e58001b3e22fb", s.commands.items().get(0)[1]);
    s = Pop3Parser.parse(lines("APOP mrose c4c9334bac560ecc979e58001b3e22fb"),
        lines("+OK ready <1@x>", "+OK maildrop has 1 message"), new MailTestContext(false));
    assertEquals("mrose ***", s.commands.items().get(0)[1]);
  }

  @Test
  public void testAuthPlain() {
    String response = Base64.getEncoder().encodeToString("\0joe\0pw".getBytes(StandardCharsets.UTF_8));
    Pop3Session s = Pop3Parser.parse(lines("AUTH PLAIN", response, "STAT"),
        lines("+OK hi", "+ ", "+OK welcome", "+OK 0 0"), new MailTestContext(true));
    assertEquals("PLAIN", s.credentials.mechanism);
    assertEquals("joe", s.credentials.username);
    assertEquals("pw", s.credentials.password);
    assertEquals(2, s.commands.items().size());
    assertEquals(Integer.valueOf(0), s.messageCount);
  }

  @Test
  public void testStls() {
    byte[] client = TestPop3Parser.concat(lines("STLS"), new byte[] {0x16, 0x03, 0x01, '\n'});
    byte[] server = TestPop3Parser.concat(lines("+OK hi", "+OK begin TLS"), new byte[] {0x16, 0x03, 0x03, '\n'});
    MailTestContext context = new MailTestContext(false);
    Pop3Session s = Pop3Parser.parse(client, server, context);
    assertTrue(s.tlsStarted);
    assertEquals(2, s.replies.items().size());
    assertTrue(context.warnings.toString(), context.warnings.isEmpty());
  }

  @Test
  public void testNotPop3() {
    MailTestContext context = new MailTestContext(false);
    assertNull(Pop3Parser.parse(lines("SSH-2.0-x"), lines("SSH-2.0-y"), context));
    assertNull(Pop3Parser.parse(new byte[0], new byte[0], context));
    assertNull(Pop3Parser.parse(lines("hello"), lines("220 smtp.example.com ESMTP"), context));
    assertTrue(context.warnings.isEmpty());
  }

  @Test
  public void testTruncatedRetr() {
    MailTestContext context = new MailTestContext(false);
    Pop3Session s = Pop3Parser.parse(lines("RETR 1"), lines("+OK hi", "+OK 50 octets", "Subject: cut", "", "par"),
        context);
    assertEquals(1, s.retrieved.items().size());
    assertEquals("cut", s.retrieved.items().get(0).headers.subject);
    assertEquals(Arrays.asList("multi-line response truncated at byte 23 of server stream"), context.warnings);
  }

  @Test
  public void testMalformedResponseWarns() {
    MailTestContext context = new MailTestContext(false);
    Pop3Session s = Pop3Parser.parse(lines("USER a", "PASS b"), lines("+OK hi", "what?", "+OK"), context);
    assertEquals(2, s.commands.items().size());
    assertEquals(1, s.replies.items().size());
    assertEquals(Arrays.asList("unparseable response at byte 8 of server stream"), context.warnings);
  }

  static byte[] concat(byte[] a, byte[] b) {
    byte[] out = Arrays.copyOf(a, a.length + b.length);
    System.arraycopy(b, 0, out, a.length, b.length);
    return out;
  }
}
