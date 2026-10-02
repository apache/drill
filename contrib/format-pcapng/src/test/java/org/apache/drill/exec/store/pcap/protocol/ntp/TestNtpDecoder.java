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
package org.apache.drill.exec.store.pcap.protocol.ntp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.TestPackets;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestNtpDecoder extends BaseTest {
  private static final long NTP_UNIX_OFFSET = 2208988800L;

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

  /** A 48-byte header; timestamps are Unix seconds plus half a second, 0 meaning a zero timestamp. */
  static byte[] header(int first, int stratum, int poll, int precision, int rootDelay, int rootDispersion,
                       byte[] refId, long ref, long origin, long receive, long transmit) {
    ByteBuffer b = ByteBuffer.allocate(48).put((byte) first).put((byte) stratum).put((byte) poll).put((byte) precision)
        .putInt(rootDelay).putInt(rootDispersion).put(refId);
    for (long t : new long[] {ref, origin, receive, transmit}) {
      if (t == 0) {
        b.putLong(0);
      } else {
        b.putInt((int) (t + NTP_UNIX_OFFSET)).putInt(0x80000000);
      }
    }
    return b.array();
  }

  @Test
  public void testServerResponse() {
    byte[] data = header(0x24, 2, 6, -20, 0x0800, 0x00010000, new byte[] {(byte) 192, (byte) 168, 1, 1},
        1704164000L, 1704164645L, 1704164645L, 1704164646L);
    NtpMessage m = NtpParser.parse(data, new Context());
    assertEquals(0, (int) m.leapIndicator);
    assertEquals(4, m.version);
    assertEquals("server", m.mode);
    assertEquals(2, (int) m.stratum);
    assertEquals(6, (int) m.poll);
    assertEquals(-20, (int) m.precision);
    assertEquals(0.03125, m.rootDelay, 1e-9);
    assertEquals(1.0, m.rootDispersion, 1e-9);
    assertEquals("192.168.1.1", m.referenceId);
    assertEquals(Instant.ofEpochMilli(1704164000500L), m.referenceTime);
    assertEquals(Instant.ofEpochMilli(1704164645500L), m.originTime);
    assertEquals(Instant.ofEpochMilli(1704164646500L), m.transmitTime);
  }

  @Test
  public void testClientRequestHasNullTimestamps() {
    byte[] data = header(0xE3, 0, 3, -6, 0, 0, new byte[4], 0, 0, 0, 1704164645L);
    NtpMessage m = NtpParser.parse(data, new Context());
    assertEquals(3, (int) m.leapIndicator);
    assertEquals("client", m.mode);
    assertNull(m.referenceId);
    assertNull(m.referenceTime);
    assertNull(m.originTime);
    assertNull(m.receiveTime);
    assertEquals(Instant.ofEpochMilli(1704164645500L), m.transmitTime);
  }

  @Test
  public void testKissCodeAndRefId() {
    assertEquals("RATE", NtpParser.parse(header(0x24, 0, 6, -20, 0, 0, "RATE".getBytes(StandardCharsets.US_ASCII),
        0, 0, 0, 1704164645L), new Context()).referenceId);
    assertEquals("GPS", NtpParser.parse(header(0x24, 1, 6, -20, 0, 0, new byte[] {'G', 'P', 'S', 0},
        0, 0, 0, 1704164645L), new Context()).referenceId);
  }

  @Test
  public void testExtensionsAreIgnored() {
    byte[] data = ByteBuffer.allocate(68).put(header(0x23, 0, 6, -20, 0, 0, new byte[4], 0, 0, 0, 1704164645L)).array();
    assertEquals("client", NtpParser.parse(data, new Context()).mode);
  }

  @Test
  public void testNotNtp() {
    // Version 0, version 5, too short, reserved mode 0
    assertNull(NtpParser.parse(header(0x03, 2, 6, -20, 0, 0, new byte[4], 0, 0, 0, 1L), new Context()));
    assertNull(NtpParser.parse(header(0x2B, 2, 6, -20, 0, 0, new byte[4], 0, 0, 0, 1L), new Context()));
    assertNull(NtpParser.parse(new byte[47], new Context()));
    assertNull(NtpParser.parse(new byte[] {0x23, 0, 0}, new Context()));
    assertNull(NtpParser.parse(header(0x20, 2, 6, -20, 0, 0, new byte[4], 0, 0, 0, 1L), new Context()));
    assertNull(NtpParser.parse("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n123456789".getBytes(), new Context()));
  }

  @Test
  public void testPrivateModeMonlistRequest() {
    // ntpdc monlist: response bit 0, version 2, mode 7, implementation 3, request code 42
    byte[] data = ByteBuffer.allocate(8).put((byte) 0x17).put((byte) 0).put((byte) 3).put((byte) 42)
        .putShort((short) 0).putShort((short) 0).array();
    NtpMessage m = NtpParser.parse(data, new Context());
    assertEquals("private", m.mode);
    assertEquals(2, m.version);
    assertEquals(42, (int) m.requestCode);
    assertNull(m.stratum);
  }

  @Test
  public void testControlModeTruncatedIsMalformed() {
    // Mode 6 header claims 100 data bytes but carries none
    byte[] data = ByteBuffer.allocate(12).put((byte) 0x16).put((byte) 2).putShort((short) 1).putShort((short) 0)
        .putShort((short) 0).putShort((short) 0).putShort((short) 100).array();
    try {
      NtpParser.parse(data, new Context());
      fail("expected malformed NTP");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("truncated"));
    }
  }

  @Test
  public void testDecoderAccepts() {
    NtpDecoder decoder = new NtpDecoder();
    byte[] data = header(0x23, 0, 6, -20, 0, 0, new byte[4], 0, 0, 0, 1L);
    assertTrue(decoder.accepts(TestPackets.udp("10.0.0.1", 40000, "10.0.0.2", 123, data)));
    assertTrue(!decoder.accepts(TestPackets.udp("10.0.0.1", 40000, "10.0.0.2", 124, data)));
    assertTrue(!decoder.accepts(TestPackets.tcp("10.0.0.1", 40000, "10.0.0.2", 123, 1, TestPackets.ACK, data)));
  }
}
