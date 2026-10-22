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
package org.apache.drill.exec.store.pcap.protocol.telnet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestTelnetSessionDecoder extends BaseTest {

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

  private static final int IAC = 255;
  private static final int SE = 240;
  private static final int SB = 250;
  private static final int WILL = 251;
  private static final int DO = 253;
  private static final int ECHO = 1;
  private static final int SUPPRESS_GO_AHEAD = 3;
  private static final int TERMINAL_TYPE = 24;

  private static byte[] concat(byte[]... parts) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] p : parts) {
      out.write(p, 0, p.length);
    }
    return out.toByteArray();
  }

  private static byte[] bytes(int... values) {
    byte[] b = new byte[values.length];
    for (int i = 0; i < values.length; i++) {
      b[i] = (byte) values[i];
    }
    return b;
  }

  private static byte[] b(String s) {
    return s.getBytes(StandardCharsets.US_ASCII);
  }

  private static byte[] client() {
    return concat(
        bytes(IAC, WILL, TERMINAL_TYPE),
        bytes(IAC, SB, TERMINAL_TYPE, 0), b("xterm"), bytes(IAC, SE),
        b("alice\r\n"),
        b("secret\r\n"));
  }

  private static byte[] server() {
    return concat(
        bytes(IAC, DO, TERMINAL_TYPE),
        bytes(IAC, WILL, ECHO),
        bytes(IAC, WILL, SUPPRESS_GO_AHEAD),
        b("Ubuntu 22.04\r\nlogin: "),
        b("Password: "),
        b("Welcome\r\n"));
  }

  @Test
  public void testSession() {
    Context context = new Context(false);
    TelnetSession s = TelnetSessionDecoder.parseStreams(client(), server(), context);
    assertEquals("alice\r\nsecret\r\n", s.clientText);
    assertEquals("Ubuntu 22.04\r\nlogin: Password: Welcome\r\n", s.serverText);
    assertEquals("xterm", s.terminalType);
    assertEquals("alice", s.loginName);
    assertTrue(s.passwordPresent);
    assertNull(s.password);
    assertEquals(4, s.options.size());
    assertEquals("TERMINAL_TYPE", s.options.get(0).option);
    assertEquals("WILL", s.options.get(0).negotiation);
    assertEquals("ECHO", s.options.get(2).option);
    assertEquals("DO", s.options.get(1).negotiation);
    assertTrue(context.warnings.isEmpty());
  }

  @Test
  public void testExposedPassword() {
    Context context = new Context(true);
    TelnetSession s = TelnetSessionDecoder.parseStreams(client(), server(), context);
    assertEquals("secret", s.password);
  }

  @Test
  public void testNotTelnet() {
    // TLS-looking bytes on port 23: no IAC and no login prompt
    byte[] tls = bytes(0x16, 0x03, 0x01, 0x00, 0x10, 0x01, 0x00, 0x00, 0x0C);
    assertNull(TelnetSessionDecoder.parseStreams(tls, new byte[0], new Context(false)));
    assertNull(TelnetSessionDecoder.parseStreams(new byte[0], new byte[0], new Context(false)));
  }

  @Test
  public void testUnterminatedSubnegotiationWarns() {
    Context context = new Context(false);
    byte[] client = concat(bytes(IAC, SB, TERMINAL_TYPE, 0), b("xterm")); // no IAC SE
    TelnetSession s = TelnetSessionDecoder.parseStreams(client, new byte[0], context);
    assertNull(s.terminalType);
    assertEquals(1, context.warnings.size());
    assertTrue(context.warnings.get(0).contains("unterminated subnegotiation"));
  }
}
