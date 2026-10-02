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
package org.apache.drill.exec.store.pcap.protocol.dns;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestDnsSessionDecoder extends BaseTest {

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

  private static byte[] name(String dotted) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (String label : dotted.split("\\.")) {
      out.write(label.length());
      out.write(label.getBytes(StandardCharsets.US_ASCII), 0, label.length());
    }
    out.write(0);
    return out.toByteArray();
  }

  /** A message with one question and the given number of A answers, framed with its 2-byte length. */
  static byte[] framed(int id, boolean response, String qname, int qtype, int answers) {
    ByteArrayOutputStream body = new ByteArrayOutputStream();
    byte[] q = name(qname);
    body.write(q, 0, q.length);
    body.write(ByteBuffer.allocate(4).putShort((short) qtype).putShort((short) 1).array(), 0, 4);
    for (int i = 0; i < answers; i++) {
      byte[] rr = ByteBuffer.allocate(16).putShort((short) 0xC00C).putShort((short) 1).putShort((short) 1)
          .putInt(300).putShort((short) 4).put(new byte[] {10, 0, (byte) (i / 256), (byte) i}).array();
      body.write(rr, 0, rr.length);
    }
    byte[] b = body.toByteArray();
    ByteBuffer m = ByteBuffer.allocate(14 + b.length).putShort((short) (12 + b.length)).putShort((short) id)
        .putShort((short) (response ? 0x8400 : 0x0000)).putShort((short) 1).putShort((short) answers)
        .putShort((short) 0).putShort((short) 0).put(b);
    return m.array();
  }

  private static DnsSession parse(byte[] client, byte[] server, DecoderContext context) {
    return DnsSessionDecoder.parseStreams(client, -1, server, -1, context);
  }

  private static byte[] concat(byte[]... parts) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] p : parts) {
      out.write(p, 0, p.length);
    }
    return out.toByteArray();
  }

  @Test
  public void testQueriesAndAnswers() {
    Context context = new Context();
    byte[] client = concat(framed(1, false, "example.com", 1, 0), framed(2, false, "example.org", 28, 0));
    byte[] server = concat(framed(1, true, "example.com", 1, 2), framed(2, true, "example.org", 28, 0));
    DnsSession s = parse(client, server, context);
    assertEquals(2, s.queries.size());
    assertEquals(2, s.queries.get(1).transactionId);
    assertEquals("example.org", s.queries.get(1).name);
    assertEquals("AAAA", s.queries.get(1).type);
    assertEquals(2, s.answers.size());
    assertEquals("example.com", s.answers.get(0).name);
    assertEquals("10.0.0.1", s.answers.get(1).data);
    assertEquals(300L, s.answers.get(0).ttl);
    assertEquals(2, s.clientMessages);
    assertEquals(2, s.serverMessages);
    assertFalse(s.isZoneTransfer);
    assertTrue(context.warnings.isEmpty());
  }

  @Test
  public void testZoneTransferCapsAnswers() {
    Context context = new Context();
    byte[] client = framed(7, false, "example.com", 252, 0);
    // Two server messages of 50 answers each: 100 in all, capped at 64
    byte[] server = concat(framed(7, true, "example.com", 252, 50), framed(7, true, "example.com", 252, 50));
    DnsSession s = parse(client, server, context);
    assertTrue(s.isZoneTransfer);
    assertEquals(64, s.answers.size());
    assertEquals(2, s.serverMessages);
    assertEquals(Arrays.asList("answers truncated to 64"), context.warnings);
  }

  @Test
  public void testIxfrIsZoneTransfer() {
    DnsSession s = parse(framed(7, false, "example.com", 251, 0), new byte[0], new Context());
    assertTrue(s.isZoneTransfer);
  }

  @Test
  public void testRepeatedParserWarningsAppearOnce() {
    Context context = new Context();
    byte[] server = concat(framed(7, true, "example.com", 252, 70), framed(7, true, "example.com", 252, 70));
    parse(framed(7, false, "example.com", 252, 0), server, context);
    assertEquals(Arrays.asList("answers truncated to 64"), context.warnings);
  }

  @Test
  public void testNotDns() {
    Context context = new Context();
    byte[] http = "GET / HTTP/1.1\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    assertNull(parse(http, http, context));
    assertNull(parse(new byte[0], new byte[0], context));
    assertTrue(context.warnings.isEmpty());
  }

  @Test
  public void testTruncatedResponseWarns() {
    Context context = new Context();
    byte[] response = framed(1, true, "example.com", 1, 1);
    // Keep the length prefix but drop the last 3 bytes of the address, as if the record were cut
    byte[] cut = Arrays.copyOf(response, response.length - 3);
    ByteBuffer.wrap(cut).putShort(0, (short) (cut.length - 2));
    DnsSession s = parse(framed(1, false, "example.com", 1, 0), cut, context);
    assertEquals(1, s.queries.size());
    assertEquals(0, s.answers.size());
    assertEquals(Arrays.asList("server message 1: truncated answer 1: needs 4 bytes at offset 41"), context.warnings);
  }

  @Test
  public void testMessageCutByEndOfStreamWarns() {
    Context context = new Context();
    byte[] client = framed(1, false, "example.com", 1, 0);
    byte[] server = framed(1, true, "example.com", 1, 1);
    DnsSession s = parse(concat(client, Arrays.copyOf(client, 5)), server, context);
    assertEquals(1, s.clientMessages);
    assertEquals(Arrays.asList("truncated message in client stream at byte " + client.length), context.warnings);
  }

  @Test
  public void testGapWarns() {
    Context context = new Context();
    byte[] client = framed(1, false, "example.com", 1, 0);
    byte[] withPart = concat(client, Arrays.copyOf(client, 5));
    DnsSessionDecoder.parseStreams(withPart, withPart.length, new byte[0], -1, context);
    assertEquals(Arrays.asList("stopped at missing data in client stream at byte " + withPart.length),
        context.warnings);
  }

  @Test
  public void testMalformedFirstMessageThrows() {
    byte[] client = framed(1, false, "example.com", 1, 0);
    byte[] response = framed(1, true, "example.com", 1, 2);
    byte[] cut = Arrays.copyOf(response, response.length - 3);
    ByteBuffer.wrap(cut).putShort(0, (short) (cut.length - 2));
    // Nothing else parsed in the session: the session is DNS but broken
    try {
      parse(new byte[0], cut, new Context());
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("server message 1: truncated answer 2: needs 4 bytes at offset 57", e.getMessage());
    }
    assertEquals(1, parse(client, new byte[0], new Context()).clientMessages);
  }
}
