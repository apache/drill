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
package org.apache.drill.exec.store.pcap.protocol.mail;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.nio.charset.StandardCharsets;

import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestMailHeaders extends BaseTest {

  private static MailHeaders parse(String s) {
    byte[] b = s.getBytes(StandardCharsets.UTF_8);
    return MailHeaders.parse(b, 0, b.length);
  }

  @Test
  public void testBasicHeaders() {
    MailHeaders h = parse("From: Alice <alice@example.com>\r\nTo: bob@example.com,\r\n carol@example.com\r\n"
        + "CC: dave@example.com\r\nSubject: Hello\r\nDate: Tue, 2 Jan 2024 03:04:05 +0000\r\n"
        + "Message-ID: <1@example.com>\r\n\r\nFrom: not-a-header@example.com\r\n");
    assertEquals("Alice <alice@example.com>", h.from);
    assertEquals("bob@example.com, carol@example.com", h.to);
    assertEquals("dave@example.com", h.cc);
    assertEquals("Hello", h.subject);
    assertEquals("Tue, 2 Jan 2024 03:04:05 +0000", h.date);
    assertEquals("<1@example.com>", h.messageId);
  }

  @Test
  public void testFirstOccurrenceWinsAndMissingIsNull() {
    MailHeaders h = parse("Subject: one\nSubject: two\n");
    assertEquals("one", h.subject);
    assertNull(h.from);
  }

  @Test
  public void testEncodedWords() {
    assertEquals("Grüße aus Köln", MailHeaders.decodeWords("=?utf-8?B?R3LDvMOfZSBhdXMgS8O2bG4=?="));
    assertEquals("Grüße aus", MailHeaders.decodeWords("=?UTF-8?Q?Gr=C3=BC=C3=9Fe_aus?="));
    // Whitespace between adjacent encoded words is dropped
    assertEquals("ab", MailHeaders.decodeWords("=?utf-8?Q?a?= \r\n =?utf-8?Q?b?="));
    assertEquals("Re: ok", MailHeaders.decodeWords("Re: =?utf-8?q?ok?="));
    // Other charsets and malformed words keep the raw text
    assertEquals("=?iso-2022-jp?B?GyRC?=", MailHeaders.decodeWords("=?iso-2022-jp?B?GyRC?="));
    assertEquals("=?utf-8?B?***?=", MailHeaders.decodeWords("=?utf-8?B?***?="));
    assertEquals("=?utf-8?Q?=ZZ?=", MailHeaders.decodeWords("=?utf-8?Q?=ZZ?="));
  }

  @Test
  public void testFoldedEncodedSubject() {
    MailHeaders h = parse("Subject: =?utf-8?B?R3LDvMOfZQ==?=\r\n =?utf-8?Q?_aus?=\r\n\r\n");
    assertEquals("Grüße aus", h.subject);
  }

  @Test
  public void testLongValueIsCapped() {
    StringBuilder b = new StringBuilder("Subject: ");
    for (int i = 0; i < 10000; i++) {
      b.append('x');
    }
    MailHeaders h = parse(b + "\r\n\r\n");
    assertEquals(MailLines.MAX_STRING, h.subject.length());
  }
}
