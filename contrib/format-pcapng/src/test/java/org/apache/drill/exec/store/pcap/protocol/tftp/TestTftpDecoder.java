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
package org.apache.drill.exec.store.pcap.protocol.tftp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.TestPackets;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestTftpDecoder extends BaseTest {

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

  private final TftpDecoder decoder = new TftpDecoder();

  /** Opcode followed by the text, in which '|' stands for a NUL. */
  private static byte[] message(int opcode, String text) {
    byte[] body = text.replace('|', '\0').getBytes(StandardCharsets.ISO_8859_1);
    byte[] out = new byte[2 + body.length];
    out[0] = (byte) (opcode >> 8);
    out[1] = (byte) opcode;
    System.arraycopy(body, 0, out, 2, body.length);
    return out;
  }

  private TftpMessage parse(byte[] payload, Context context) {
    return decoder.parse(TestPackets.udp("10.0.0.1", 50000, "10.0.0.2", 69, payload), payload, context);
  }

  private void assertMalformed(byte[] payload, String message) {
    try {
      parse(payload, new Context());
      fail("expected malformed TFTP");
    } catch (IllegalArgumentException e) {
      assertEquals(message, e.getMessage());
    }
  }

  @Test
  public void testAccepts() {
    assertTrue(decoder.accepts(TestPackets.udp("10.0.0.1", 50000, "10.0.0.2", 69, new byte[1])));
    assertTrue(decoder.accepts(TestPackets.udp("10.0.0.2", 69, "10.0.0.1", 50000, new byte[1])));
    assertFalse(decoder.accepts(TestPackets.udp("10.0.0.1", 50000, "10.0.0.2", 50001, new byte[1])));
    assertFalse(decoder.accepts(TestPackets.tcp("10.0.0.1", 50000, "10.0.0.2", 69, 1, TestPackets.ACK, new byte[1])));
  }

  @Test
  public void testReadRequestWithOptions() {
    TftpMessage m = parse(message(1, "boot/pxelinux.0|OCTET|blksize|1428|tsize|0|"), new Context());
    assertEquals("RRQ", m.opcode);
    assertEquals("boot/pxelinux.0", m.filename);
    assertEquals("octet", m.mode);
    assertEquals(2, m.options.size());
    assertEquals("blksize", m.options.get(0)[0]);
    assertEquals("1428", m.options.get(0)[1]);
    assertEquals("tsize", m.options.get(1)[0]);
  }

  @Test
  public void testWriteRequest() {
    TftpMessage m = parse(message(2, "config.txt|netascii|"), new Context());
    assertEquals("WRQ", m.opcode);
    assertEquals("netascii", m.mode);
    assertTrue(m.options.isEmpty());
  }

  @Test
  public void testDataAckErrorOack() {
    byte[] data = new byte[4 + 100];
    data[1] = 3;
    data[3] = 7;
    TftpMessage d = parse(data, new Context());
    assertEquals("DATA", d.opcode);
    assertEquals(Integer.valueOf(7), d.block);
    assertEquals(Integer.valueOf(100), d.dataLength);

    TftpMessage a = parse(new byte[] {0, 4, 1, 2}, new Context());
    assertEquals("ACK", a.opcode);
    assertEquals(Integer.valueOf(258), a.block);

    TftpMessage e = parse(message(5, "\0\1File not found|"), new Context());
    assertEquals("ERROR", e.opcode);
    assertEquals(Integer.valueOf(1), e.errorCode);
    assertEquals("File not found", e.errorMessage);

    TftpMessage o = parse(message(6, "blksize|1428|"), new Context());
    assertEquals("OACK", o.opcode);
    assertEquals("1428", o.options.get(0)[1]);
  }

  @Test
  public void testNotTftp() {
    Context context = new Context();
    assertNull(parse(new byte[] {0}, context));
    assertNull(parse(message(9, "x|octet|"), context));
    // Unknown mode
    assertNull(parse(message(1, "file|binary|"), context));
    // No terminator after the file name
    assertNull(parse(message(1, "file"), context));
    // Control characters in the file name
    assertNull(parse(message(1, "fi\1le|octet|"), context));
    // ACK must be exactly 4 bytes
    assertNull(parse(new byte[] {0, 4, 0, 1, 9}, context));
    // Error code 99 is not assigned
    assertNull(parse(message(5, "\0\143oops|"), context));
    assertNull(parse("GET / HTTP/1.1\r\n".getBytes(), context));
  }

  @Test
  public void testMalformed() {
    assertMalformed(message(1, "file|octet|blksize|14"), "unterminated option value");
    assertMalformed(message(1, "file|oct"), "request truncated in mode");
    assertMalformed(message(5, "\0\1File not"), "unterminated error message");
    assertMalformed(message(1, "file|octet|blksize|"), "option blksize has no value");
  }

  @Test
  public void testOptionCap() {
    StringBuilder text = new StringBuilder("f|octet|");
    for (int i = 0; i < 70; i++) {
      text.append("o").append(i).append("|v|");
    }
    Context context = new Context();
    TftpMessage m = parse(message(1, text.toString()), context);
    assertEquals(64, m.options.size());
    assertEquals(Arrays.asList("options truncated to 64"), context.warnings);
  }
}
