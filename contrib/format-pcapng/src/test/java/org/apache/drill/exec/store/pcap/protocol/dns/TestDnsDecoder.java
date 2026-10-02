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
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestDnsDecoder extends BaseTest {

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

  /** Header plus raw body. */
  private static byte[] message(int id, int flags, int qd, int an, int ns, int ar, byte[] body) {
    return ByteBuffer.allocate(12 + body.length).putShort((short) id).putShort((short) flags)
        .putShort((short) qd).putShort((short) an).putShort((short) ns).putShort((short) ar).put(body).array();
  }

  private static byte[] name(String dotted) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (String label : dotted.split("\\.")) {
      out.write(label.length());
      out.write(label.getBytes(), 0, label.length());
    }
    out.write(0);
    return out.toByteArray();
  }

  private static byte[] concat(byte[]... parts) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] p : parts) {
      out.write(p, 0, p.length);
    }
    return out.toByteArray();
  }

  private static byte[] question(String n, int type) {
    return concat(name(n), ByteBuffer.allocate(4).putShort((short) type).putShort((short) 1).array());
  }

  /** Answer whose name is a compression pointer to offset 12 (the question name). */
  private static byte[] answerA(int ttl, byte[] address) {
    return ByteBuffer.allocate(2 + 10 + address.length).put((byte) 0xC0).put((byte) 12)
        .putShort((short) 1).putShort((short) 1).putInt(ttl).putShort((short) address.length).put(address).array();
  }

  @Test
  public void testQueryAndResponse() {
    byte[] response = message(0x1234, 0x8180, 1, 1, 0, 0,
        concat(question("example.com", 1), answerA(300, new byte[] {93, (byte) 184, (byte) 216, 34})));
    DnsMessage m = DnsParser.parse(response, 0, new Context());
    assertEquals(0x1234, m.transactionId);
    assertTrue(m.isResponse);
    assertTrue(m.recursionDesired);
    assertTrue(m.recursionAvailable);
    assertEquals("example.com", m.questions.get(0).name);
    assertEquals("A", m.questions.get(0).type);
    assertEquals("example.com", m.answers.get(0).name);
    assertEquals(300L, m.answers.get(0).ttl);
    assertEquals("93.184.216.34", m.answers.get(0).data);
  }

  @Test
  public void testNotDns() {
    assertNull(DnsParser.parse("GET / HTTP/1.1\r\nHost: x\r\n\r\n".getBytes(), 0, new Context()));
    assertNull(DnsParser.parse(new byte[] {1, 2, 3}, 0, new Context()));
    assertNull(DnsParser.parse(message(1, 0, 0, 0, 0, 0, new byte[0]), 0, new Context()));
    // Opcode 15 is not assigned
    assertNull(DnsParser.parse(message(1, 15 << 11, 1, 0, 0, 0, question("a.b", 1)), 0, new Context()));
  }

  @Test
  public void testTruncatedAnswerIsMalformedDns() {
    byte[] full = message(1, 0x8180, 1, 1, 0, 0, concat(question("example.com", 1), answerA(300, new byte[4])));
    byte[] cut = java.util.Arrays.copyOf(full, full.length - 3);
    try {
      DnsParser.parse(cut, 0, new Context());
      fail("expected malformed DNS");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("answer 1"));
    }
  }

  @Test
  public void testCompressionPointerLoop() {
    // Answer name points to itself
    byte[] loop = ByteBuffer.allocate(12).put((byte) 0xC0).put((byte) 29).putShort((short) 1).putShort((short) 1)
        .putInt(0).putShort((short) 0).array();
    byte[] m = message(1, 0x8180, 1, 1, 0, 0, concat(question("a.b", 1), loop));
    try {
      DnsParser.parse(m, 0, new Context());
      fail("expected malformed DNS");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("pointer"));
    }
  }

  @Test
  public void testRecordCap() {
    ByteArrayOutputStream body = new ByteArrayOutputStream();
    byte[] q = question("a.b", 1);
    body.write(q, 0, q.length);
    for (int i = 0; i < 70; i++) {
      byte[] a = answerA(1, new byte[4]);
      body.write(a, 0, a.length);
    }
    Context context = new Context();
    DnsMessage m = DnsParser.parse(message(1, 0x8180, 1, 70, 0, 0, body.toByteArray()), 0, context);
    assertEquals(64, m.answers.size());
    assertEquals(1, context.warnings.size());
  }

  @Test
  public void testRecordTypes() {
    byte[] mx = ByteBuffer.allocate(2 + 10 + 2 + 2).put((byte) 0xC0).put((byte) 12).putShort((short) 15).putShort((short) 1)
        .putInt(60).putShort((short) 4).putShort((short) 10).put((byte) 0xC0).put((byte) 12).array();
    DnsMessage m = DnsParser.parse(message(1, 0x8180, 1, 1, 0, 0, concat(question("example.com", 15), mx)), 0, new Context());
    assertEquals("MX", m.answers.get(0).type);
    assertEquals("10 example.com", m.answers.get(0).data);
    assertFalse(m.truncated);
  }
}
