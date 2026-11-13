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
package org.apache.drill.exec.store.pcap.protocol.syslog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.TestPackets;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestSyslogDecoder extends BaseTest {

  private static final class Context implements DecoderContext {
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

  private static SyslogMessage parse(String text) {
    return SyslogParser.parse(text.getBytes(StandardCharsets.UTF_8), new Context());
  }

  @Test
  public void testRfc5424() {
    SyslogMessage m = parse("<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 "
        + "[exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"] An application event\n");
    assertEquals(20, m.facility);
    assertEquals("local4", m.facilityName);
    assertEquals(5, m.severity);
    assertEquals("notice", m.severityName);
    assertEquals(1, (int) m.version);
    assertEquals("2003-10-11T22:14:15.003Z", m.timestampText);
    assertEquals("mymachine.example.com", m.hostname);
    assertEquals("evntslog", m.appName);
    assertNull(m.procId);
    assertEquals("ID47", m.msgId);
    assertEquals("[exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"]", m.structuredData);
    assertEquals("An application event", m.message);
  }

  @Test
  public void testRfc5424NilValuesAndBom() {
    SyslogMessage m = parse("<34>1 - - su 77 - [a@1 x=\"q\\]\"][b@2] ﻿'su root' failed");
    assertEquals(4, m.facility);
    assertEquals("auth", m.facilityName);
    assertEquals(2, m.severity);
    assertEquals("crit", m.severityName);
    assertNull(m.timestampText);
    assertNull(m.hostname);
    assertEquals("su", m.appName);
    assertEquals("77", m.procId);
    assertNull(m.msgId);
    assertEquals("[a@1 x=\"q\\]\"][b@2]", m.structuredData);
    assertEquals("'su root' failed", m.message);

    m = parse("<14>1 2024-01-02T03:04:05Z host app - - -");
    assertNull(m.structuredData);
    assertNull(m.message);
  }

  @Test
  public void testRfc3164() {
    SyslogMessage m = parse("<38>Jan  2 03:04:05 gateway sshd[1234]: Accepted password for root");
    assertEquals(4, m.facility);
    assertEquals(6, m.severity);
    assertNull(m.version);
    assertEquals("Jan  2 03:04:05", m.timestampText);
    assertEquals("gateway", m.hostname);
    assertEquals("sshd", m.appName);
    assertEquals("1234", m.procId);
    assertEquals("Accepted password for root", m.message);

    // No hostname
    m = parse("<13>Oct 11 22:14:15 su: 'su root' failed for lonvick on /dev/pts/8");
    assertNull(m.hostname);
    assertEquals("su", m.appName);
    assertNull(m.procId);
    assertEquals("'su root' failed for lonvick on /dev/pts/8", m.message);

    // No tag
    m = parse("<13>Oct 11 22:14:15 host free text");
    assertEquals("host", m.hostname);
    assertNull(m.appName);
    assertEquals("free text", m.message);
  }

  @Test
  public void testPriorityOnly() {
    SyslogMessage m = parse("<0>kernel panic");
    assertEquals(0, m.facility);
    assertEquals("kern", m.facilityName);
    assertEquals("emerg", m.severityName);
    assertNull(m.timestampText);
    assertEquals("kernel panic", m.message);
  }

  @Test
  public void testNotSyslog() {
    assertNull(parse("hello"));
    assertNull(parse("<192>too high"));
    assertNull(parse("<1234>four digits"));
    assertNull(parse("<>empty"));
    assertNull(parse("<12"));
    assertNull(parse("<a>letters"));
    assertNull(SyslogParser.parse(new byte[0], new Context()));
  }

  @Test
  public void testRfc5424MissingFieldsIsMalformed() {
    try {
      parse("<14>1 2024-01-02T03:04:05Z host");
      fail("expected malformed syslog");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("truncated"));
    }
    try {
      parse("<14>1 2024-01-02T03:04:05Z host app - - [unterminated x=\"1\"");
      fail("expected malformed syslog");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("structured data"));
    }
  }

  @Test
  public void testDecoderAccepts() {
    SyslogDecoder decoder = new SyslogDecoder();
    byte[] data = "<13>hi".getBytes(StandardCharsets.UTF_8);
    assertTrue(decoder.accepts(TestPackets.udp("10.0.0.1", 40000, "10.0.0.2", 514, data)));
    assertTrue(!decoder.accepts(TestPackets.udp("10.0.0.1", 40000, "10.0.0.2", 515, data)));
    assertTrue(!decoder.accepts(TestPackets.tcp("10.0.0.1", 40000, "10.0.0.2", 514, 1, TestPackets.ACK, data)));
  }
}
