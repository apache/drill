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
package org.apache.drill.exec.store.pcap.protocol.dhcpv6;

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

public class TestDhcpv6Decoder extends BaseTest {

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

  private static final byte[] DUID = hex("000100012a2b2c2d020000000001");
  private static final byte[] ADDRESS = hex("20010db8000000000000000000000100");
  private static final byte[] DNS = hex("20010db8000000000000000000000053");

  private final Dhcpv6Decoder decoder = new Dhcpv6Decoder();

  private static byte[] hex(String s) {
    byte[] out = new byte[s.length() / 2];
    for (int i = 0; i < out.length; i++) {
      out[i] = (byte) Integer.parseInt(s.substring(2 * i, 2 * i + 2), 16);
    }
    return out;
  }

  private static byte[] concat(byte[]... parts) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] p : parts) {
      out.write(p, 0, p.length);
    }
    return out.toByteArray();
  }

  private static byte[] option(int code, byte[] value) {
    return ByteBuffer.allocate(4 + value.length).putShort((short) code).putShort((short) value.length).put(value).array();
  }

  private static byte[] message(int type, int xid, byte[]... options) {
    return concat(new byte[] {(byte) type, (byte) (xid >> 16), (byte) (xid >> 8), (byte) xid}, concat(options));
  }

  private static byte[] iaNa(byte[] address) {
    byte[] iaAddr = option(5, ByteBuffer.allocate(24).put(address).putInt(3600).putInt(7200).array());
    return option(3, concat(ByteBuffer.allocate(12).putInt(1).putInt(1800).putInt(2880).array(), iaAddr));
  }

  private Dhcpv6Message parse(byte[] payload, Context context) {
    return decoder.parse(TestPackets.udp("10.0.0.1", 546, "10.0.0.2", 547, payload), payload, context);
  }

  @Test
  public void testAccepts() {
    assertTrue(decoder.accepts(TestPackets.udp("10.0.0.1", 546, "10.0.0.2", 547, new byte[1])));
    assertTrue(decoder.accepts(TestPackets.udp("10.0.0.1", 547, "10.0.0.2", 547, new byte[1])));
    assertFalse(decoder.accepts(TestPackets.udp("10.0.0.1", 67, "10.0.0.2", 68, new byte[1])));
  }

  @Test
  public void testReply() {
    byte[] payload = message(7, 0xABCDEF,
        option(1, DUID),
        option(2, hex("0003000102000000ffff")),
        iaNa(ADDRESS),
        option(23, DNS),
        option(24, hex("076578616d706c65036f72670004636f7270076578616d706c6500")),
        option(13, concat(new byte[] {0, 0}, "ok".getBytes())));
    Dhcpv6Message m = parse(payload, new Context());
    assertEquals("REPLY", m.messageType);
    assertEquals(Integer.valueOf(0xABCDEF), m.transactionId);
    assertEquals("000100012a2b2c2d020000000001", m.clientDuid);
    assertEquals("0003000102000000ffff", m.serverDuid);
    assertEquals(Arrays.asList("2001:db8:0:0:0:0:0:100"), m.iaAddresses);
    assertEquals(Arrays.asList("2001:db8:0:0:0:0:0:53"), m.dnsServers);
    assertEquals(Arrays.asList("example.org", "corp.example"), m.domainList);
    assertEquals(Integer.valueOf(0), m.statusCode);
    assertEquals("ok", m.statusMessage);
    assertEquals(6, m.options.size());
    assertEquals(1, m.options.get(0).code);
    assertEquals("000100012a2b2c2d020000000001", m.options.get(0).value);
  }

  @Test
  public void testSolicitWithFqdnAndOro() {
    byte[] payload = message(1, 0x000102,
        option(1, DUID),
        option(8, new byte[] {0, 0}),
        option(6, new byte[] {0, 23, 0, 24}),
        option(39, hex("01046c6170740165") /* flags 1, "lapt" + label "e" without root: partial name */),
        option(25, concat(ByteBuffer.allocate(12).putInt(2).putInt(0).putInt(0).array(),
            option(26, concat(ByteBuffer.allocate(9).putInt(0).putInt(0).put((byte) 56).array(),
                hex("20010db8aabbcc000000000000000000"))))));
    Dhcpv6Message m = parse(payload, new Context());
    assertEquals("SOLICIT", m.messageType);
    assertEquals(Integer.valueOf(0x102), m.transactionId);
    assertEquals(Arrays.asList(23, 24), m.optionRequestList);
    assertEquals("lapt.e", m.fqdn);
    assertEquals(Arrays.asList("2001:db8:aabb:cc00:0:0:0:0/56"), m.iaPrefixes);
  }

  @Test
  public void testRelayForward() {
    byte[] inner = message(1, 0x123456, option(1, DUID));
    byte[] relay = concat(new byte[] {12, 0}, ADDRESS, hex("fe800000000000000000000000000001"),
        option(18, "eth0".getBytes()), option(9, inner));
    Dhcpv6Message m = parse(relay, new Context());
    assertEquals("RELAY-FORW", m.messageType);
    assertEquals("SOLICIT", m.relayedMessageType);
    assertEquals(Integer.valueOf(0), m.hopCount);
    assertEquals("2001:db8:0:0:0:0:0:100", m.linkAddress);
    assertEquals("fe80:0:0:0:0:0:0:1", m.peerAddress);
    assertEquals(Integer.valueOf(0x123456), m.transactionId);
    assertEquals("000100012a2b2c2d020000000001", m.clientDuid);
    assertEquals(1, m.options.size());
  }

  @Test
  public void testNotDhcpv6() {
    Context context = new Context();
    assertNull(parse(new byte[] {1, 2, 3}, context));
    // Type 0 is not assigned
    assertNull(parse(message(0, 1, option(1, DUID)), context));
    // No options
    assertNull(parse(message(1, 1), context));
    // First option overruns the payload
    assertNull(parse(message(1, 1, new byte[] {0, 1, 0x7f, 0}), context));
    assertNull(parse("M-SEARCH * HTTP/1.1\r\n".getBytes(), context));
  }

  @Test
  public void testTruncatedOption() {
    byte[] full = message(1, 1, option(1, DUID), option(8, new byte[] {0, 0}));
    try {
      parse(Arrays.copyOf(full, full.length - 1), new Context());
      fail("expected malformed DHCPv6");
    } catch (IllegalArgumentException e) {
      assertEquals("option 8 at offset 22 overruns the message", e.getMessage());
    }
  }

  @Test
  public void testBadDnsServerLength() {
    try {
      parse(message(7, 1, option(1, DUID), option(23, new byte[10])), new Context());
      fail("expected malformed DHCPv6");
    } catch (IllegalArgumentException e) {
      assertEquals("option 23 has length 10", e.getMessage());
    }
  }

  @Test
  public void testOptionCap() {
    byte[][] options = new byte[70][];
    for (int i = 0; i < options.length; i++) {
      options[i] = option(8, new byte[] {0, 0});
    }
    Context context = new Context();
    Dhcpv6Message m = parse(message(1, 1, options), context);
    assertEquals(64, m.options.size());
    assertEquals(Arrays.asList("options truncated to 64"), context.warnings);
  }
}
