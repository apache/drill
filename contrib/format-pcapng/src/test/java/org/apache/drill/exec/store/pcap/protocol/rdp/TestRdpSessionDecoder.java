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
package org.apache.drill.exec.store.pcap.protocol.rdp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestRdpSessionDecoder extends BaseTest {

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

  /** TPKT v3 + X.224 header (LI, code, DST-REF, SRC-REF, class) wrapping the given user data. */
  private static byte[] tpkt(int code, byte[] userData) {
    int total = 4 + 7 + userData.length;
    byte[] header = bytes(0x03, 0x00, total >> 8, total & 0xFF, 6 + userData.length, code, 0, 0, 0, 0, 0);
    return concat(header, userData);
  }

  private static byte[] negReq(long protocols) {
    return bytes(0x01, 0x00, 0x08, 0x00,
        (int) (protocols & 0xFF), (int) ((protocols >> 8) & 0xFF),
        (int) ((protocols >> 16) & 0xFF), (int) ((protocols >> 24) & 0xFF));
  }

  private static byte[] cookie(String user) {
    return ("Cookie: mstshash=" + user + "\r\n").getBytes(StandardCharsets.US_ASCII);
  }

  @Test
  public void testConnectionRequest() {
    byte[] client = tpkt(0xE0, concat(cookie("alice"), negReq(0x03))); // TLS + CredSSP
    byte[] server = tpkt(0xD0, bytes(0x02, 0x00, 0x08, 0x00, 0x01, 0, 0, 0)); // NEG_RSP selected TLS
    Context context = new Context();
    RdpSession s = RdpSessionDecoder.parseStreams(client, server, context);
    assertEquals("alice", s.cookie);
    assertEquals(Arrays.asList("TLS", "CredSSP"), s.requestedProtocols);
    assertEquals("TLS", s.selectedProtocol);
    assertNull(s.negotiationFailure);
    assertTrue(context.warnings.isEmpty());
  }

  @Test
  public void testStandardRdpAndFailure() {
    byte[] client = tpkt(0xE0, negReq(0x00)); // no cookie, standard RDP security
    byte[] server = tpkt(0xD0, bytes(0x03, 0x00, 0x08, 0x00, 0x05, 0, 0, 0)); // NEG_FAILURE HYBRID_REQUIRED
    RdpSession s = RdpSessionDecoder.parseStreams(client, server, new Context());
    assertNull(s.cookie);
    assertEquals(Arrays.asList("RDP"), s.requestedProtocols);
    assertNull(s.selectedProtocol);
    assertEquals("HYBRID_REQUIRED_BY_SERVER", s.negotiationFailure);
  }

  @Test
  public void testNotRdp() {
    byte[] http = "GET / HTTP/1.0\r\n".getBytes(StandardCharsets.US_ASCII);
    assertNull(RdpSessionDecoder.parseStreams(http, new byte[0], new Context()));
    assertNull(RdpSessionDecoder.parseStreams(new byte[0], new byte[0], new Context()));
  }

  @Test
  public void testTruncatedNegReqThrows() {
    byte[] client = tpkt(0xE0, concat(cookie("alice"), bytes(0x01, 0x00, 0x08, 0x00))); // neg req cut to 4 bytes
    try {
      RdpSessionDecoder.parseStreams(client, new byte[0], new Context());
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("truncated negotiation request", e.getMessage());
    }
  }
}
