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
package org.apache.drill.exec.store.pcap.protocol.dhcp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.TestPackets;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestDhcpDecoder extends BaseTest {

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

  private final DhcpDecoder decoder = new DhcpDecoder();

  /** BOOTP header with the given op, then the magic cookie and the options bytes. */
  private static byte[] message(int op, byte[] options) {
    ByteBuffer b = ByteBuffer.allocate(240 + options.length);
    b.put((byte) op).put((byte) 1).put((byte) 6).put((byte) 0);
    b.putInt(0xDEADBEEF).putShort((short) 3).putShort((short) 0x8000);
    b.put(new byte[] {0, 0, 0, 0});                      // ciaddr
    b.put(new byte[] {(byte) 192, (byte) 168, 1, 100});  // yiaddr
    b.put(new byte[] {(byte) 192, (byte) 168, 1, 1});    // siaddr
    b.put(new byte[] {0, 0, 0, 0});                      // giaddr
    b.put(new byte[] {0x02, 0x11, 0x22, 0x33, 0x44, 0x55}).put(new byte[10]);
    b.put("bootsrv".getBytes()).put(new byte[64 - 7]);
    b.put("pxelinux.0".getBytes()).put(new byte[128 - 10]);
    b.putInt(0x63825363).put(options);
    return b.array();
  }

  private static byte[] options(int[]... options) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (int[] o : options) {
      for (int v : o) {
        out.write(v);
      }
    }
    return out.toByteArray();
  }

  private static int[] text(int code, String s) {
    int[] o = new int[2 + s.length()];
    o[0] = code;
    o[1] = s.length();
    for (int i = 0; i < s.length(); i++) {
      o[2 + i] = s.charAt(i);
    }
    return o;
  }

  private DhcpMessage parse(byte[] payload, Context context) {
    return decoder.parse(TestPackets.udp("0.0.0.0", 68, "255.255.255.255", 67, payload), payload, context);
  }

  @Test
  public void testAccepts() {
    assertTrue(decoder.accepts(TestPackets.udp("0.0.0.0", 68, "255.255.255.255", 67, new byte[1])));
    assertTrue(decoder.accepts(TestPackets.udp("10.0.0.1", 67, "10.0.0.2", 68, new byte[1])));
    assertFalse(decoder.accepts(TestPackets.udp("10.0.0.1", 5000, "10.0.0.2", 53, new byte[1])));
    assertFalse(decoder.accepts(TestPackets.tcp("10.0.0.1", 5000, "10.0.0.2", 67, 1, TestPackets.ACK, new byte[1])));
  }

  @Test
  public void testAck() {
    byte[] payload = message(2, options(
        new int[] {53, 1, 5},
        new int[] {54, 4, 192, 168, 1, 1},
        new int[] {51, 4, 0, 1, 0x51, 0x80},
        new int[] {1, 4, 255, 255, 255, 0},
        new int[] {3, 8, 192, 168, 1, 1, 192, 168, 1, 2},
        new int[] {6, 8, 8, 8, 8, 8, 1, 1, 1, 1},
        text(15, "example.org"),
        new int[] {0, 0},
        new int[] {255}));
    DhcpMessage m = parse(payload, new Context());
    assertEquals("reply", m.op);
    assertEquals("ACK", m.messageType);
    assertEquals(0xDEADBEEFL, m.transactionId);
    assertEquals("02:11:22:33:44:55", m.clientMac);
    assertEquals("0.0.0.0", m.clientIp);
    assertEquals("192.168.1.100", m.yourIp);
    assertEquals("192.168.1.1", m.serverIp);
    assertEquals("0.0.0.0", m.relayIp);
    assertEquals("192.168.1.1", m.serverId);
    assertEquals(Long.valueOf(86400), m.leaseTime);
    assertEquals("255.255.255.0", m.subnetMask);
    assertEquals(Arrays.asList("192.168.1.1", "192.168.1.2"), m.routers);
    assertEquals(Arrays.asList("8.8.8.8", "1.1.1.1"), m.dnsServers);
    assertEquals("example.org", m.domainName);
    assertEquals("bootsrv", m.serverName);
    assertEquals("pxelinux.0", m.bootFile);
    assertTrue(m.broadcast);
    assertEquals(7, m.options.size());
    assertEquals(53, m.options.get(0).code);
    assertEquals("05", m.options.get(0).value);
  }

  @Test
  public void testDiscover() {
    byte[] payload = message(1, options(
        new int[] {53, 1, 1},
        text(12, "laptop"),
        new int[] {50, 4, 192, 168, 1, 50},
        text(60, "MSFT 5.0"),
        new int[] {55, 4, 1, 3, 6, 15},
        new int[] {255}));
    DhcpMessage m = parse(payload, new Context());
    assertEquals("request", m.op);
    assertEquals("DISCOVER", m.messageType);
    assertEquals("laptop", m.hostname);
    assertEquals("192.168.1.50", m.requestedIp);
    assertEquals("MSFT 5.0", m.vendorClass);
    assertEquals(Arrays.asList(1, 3, 6, 15), m.parameterRequestList);
  }

  @Test
  public void testNotDhcp() {
    Context context = new Context();
    assertNull(parse(new byte[] {1, 2, 3}, context));
    // No magic cookie: plain BOOTP is not decoded
    byte[] bootp = message(1, new byte[] {(byte) 255});
    bootp[236] = 0;
    assertNull(parse(bootp, context));
    // Op 3 is not BOOTP
    byte[] badOp = message(3, new byte[] {(byte) 255});
    assertNull(parse(badOp, context));
    // Hardware address length over 16
    byte[] badHlen = message(1, new byte[] {(byte) 255});
    badHlen[2] = 40;
    assertNull(parse(badHlen, context));
    byte[] badHtype = message(1, new byte[] {(byte) 255});
    badHtype[1] = 0;
    assertNull(parse(badHtype, context));
  }

  @Test
  public void testTruncatedOption() {
    // Option 12 claims 20 bytes, only 3 follow
    byte[] payload = message(1, options(new int[] {53, 1, 1}, new int[] {12, 20, 'a', 'b', 'c'}));
    try {
      parse(payload, new Context());
      fail("expected malformed DHCP");
    } catch (IllegalArgumentException e) {
      assertEquals("option 12 at offset 243 overruns the message", e.getMessage());
    }
  }

  @Test
  public void testBadOptionLength() {
    byte[] payload = message(1, options(new int[] {53, 2, 1, 1}, new int[] {255}));
    try {
      parse(payload, new Context());
      fail("expected malformed DHCP");
    } catch (IllegalArgumentException e) {
      assertEquals("option 53 has length 2", e.getMessage());
    }
  }

  @Test
  public void testOptionCap() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (int i = 0; i < 70; i++) {
      out.write(224);
      out.write(1);
      out.write(i);
    }
    out.write(255);
    Context context = new Context();
    DhcpMessage m = parse(message(1, out.toByteArray()), context);
    assertEquals(64, m.options.size());
    assertEquals(Arrays.asList("options truncated to 64"), context.warnings);
  }

  @Test
  public void testMissingEndOption() {
    // Many clients pad the message; a message that simply ends without option 255 is accepted
    DhcpMessage m = parse(message(1, options(new int[] {53, 1, 3})), new Context());
    assertEquals("REQUEST", m.messageType);
  }
}
