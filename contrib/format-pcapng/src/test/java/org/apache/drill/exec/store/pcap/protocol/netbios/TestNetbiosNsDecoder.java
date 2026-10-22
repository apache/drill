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
package org.apache.drill.exec.store.pcap.protocol.netbios;

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
import org.apache.drill.exec.store.pcapng.PacketDecoder;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

public class TestNetbiosNsDecoder extends BaseTest {

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

  private final NetbiosNsDecoder decoder = new NetbiosNsDecoder();

  /** First-level encoded name: length 32, the encoded label, then the root. */
  static byte[] name(String name, int suffix) {
    byte[] raw = new byte[16];
    Arrays.fill(raw, (byte) ' ');
    byte[] chars = name.getBytes();
    System.arraycopy(chars, 0, raw, 0, chars.length);
    raw[15] = (byte) suffix;
    byte[] out = new byte[34];
    out[0] = 32;
    for (int i = 0; i < 16; i++) {
      out[1 + 2 * i] = (byte) ('A' + ((raw[i] >> 4) & 0xF));
      out[2 + 2 * i] = (byte) ('A' + (raw[i] & 0xF));
    }
    return out;
  }

  static byte[] header(int id, int flags, int qd, int an, int ns, int ar) {
    return ByteBuffer.allocate(12).putShort((short) id).putShort((short) flags).putShort((short) qd)
        .putShort((short) an).putShort((short) ns).putShort((short) ar).array();
  }

  static byte[] concat(byte[]... parts) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] p : parts) {
      out.write(p, 0, p.length);
    }
    return out.toByteArray();
  }

  static byte[] question(String n, int suffix) {
    return concat(name(n, suffix), ByteBuffer.allocate(4).putShort((short) 0x20).putShort((short) 1).array());
  }

  /** NB record with one address entry; nameBytes may be a pointer. */
  static byte[] nbRecord(byte[] nameBytes, int ttl, int nbFlags, byte[] address) {
    return concat(nameBytes, ByteBuffer.allocate(16).putShort((short) 0x20).putShort((short) 1).putInt(ttl)
        .putShort((short) 6).putShort((short) nbFlags).put(address).array());
  }

  private NetbiosNsMessage parse(byte[] payload, DecoderContext context) {
    PacketDecoder packet = TestPackets.udp("10.0.0.5", 137, "10.0.0.255", 137, payload);
    assertTrue(decoder.accepts(packet));
    return decoder.parse(packet, payload, context);
  }

  @Test
  public void testBroadcastQuery() {
    NetbiosNsMessage m = parse(concat(header(0x8001, 0x0110, 1, 0, 0, 0), question("FILESRV", 0x20)), new Context());
    assertEquals(0x8001, m.transactionId);
    assertFalse(m.isResponse);
    assertEquals("query", m.opcode);
    assertTrue(m.broadcast);
    assertEquals(1, m.questions.size());
    assertEquals("FILESRV", m.questions.get(0).name);
    assertEquals(0x20, m.questions.get(0).suffix);
    assertEquals("file_server", m.questions.get(0).suffixName);
    assertEquals("NB", m.questions.get(0).type);
  }

  @Test
  public void testPositiveResponse() {
    byte[] record = nbRecord(name("FILESRV", 0x20), 300000, 0x6000, new byte[] {10, 0, 0, 7});
    NetbiosNsMessage m = parse(concat(header(0x8001, 0x8500, 0, 1, 0, 0), record), new Context());
    assertTrue(m.isResponse);
    assertEquals(0, m.rcode);
    assertFalse(m.broadcast);
    NetbiosNsMessage.Record r = m.answers.get(0);
    assertEquals("FILESRV", r.name);
    assertEquals(0x20, r.suffix);
    assertEquals(300000L, r.ttl);
    assertEquals(Arrays.asList("10.0.0.7"), r.addresses);
    assertEquals("H", r.nodeType);
    assertFalse(r.group);
  }

  @Test
  public void testRegistrationWithPointer() {
    // The additional record's name points back to the question name at offset 12
    byte[] record = nbRecord(new byte[] {(byte) 0xC0, 12}, 300000, 0x8000, new byte[] {10, 0, 0, 5});
    NetbiosNsMessage m = parse(concat(header(0x8002, 0x2910, 1, 0, 0, 1), question("WORKGROUP", 0x00), record),
        new Context());
    assertEquals("registration", m.opcode);
    assertEquals("WORKGROUP", m.additionals.get(0).name);
    assertEquals("workstation", m.additionals.get(0).suffixName);
    assertTrue(m.additionals.get(0).group);
    assertEquals("B", m.additionals.get(0).nodeType);
  }

  @Test
  public void testNodeStatusResponse() {
    ByteBuffer rdata = ByteBuffer.allocate(1 + 18 * 2 + 46);
    rdata.put((byte) 2);
    rdata.put("FILESRV        ".getBytes()).put((byte) 0x00).putShort((short) 0x0400);
    rdata.put("FILESRV        ".getBytes()).put((byte) 0x20).putShort((short) 0x0400);
    rdata.put(new byte[] {0x00, 0x11, 0x22, 0x33, 0x44, 0x55});
    byte[] r = rdata.array();
    byte[] record = concat(name("*", 0x00), ByteBuffer.allocate(10).putShort((short) 0x21).putShort((short) 1)
        .putInt(0).putShort((short) r.length).array(), r);
    NetbiosNsMessage m = parse(concat(header(1, 0x8400, 0, 1, 0, 0), record), new Context());
    NetbiosNsMessage.Record a = m.answers.get(0);
    assertEquals("*", a.name);
    assertEquals("NBSTAT", a.type);
    assertEquals(Arrays.asList("FILESRV<00>", "FILESRV<20>"), a.names);
    assertEquals("00:11:22:33:44:55", a.macAddress);
  }

  @Test
  public void testNotNetbios() {
    assertNull(parse("hello there, this is not netbios".getBytes(), new Context()));
    assertNull(parse(new byte[] {1, 2, 3}, new Context()));
    // DNS-style name instead of an encoded one
    byte[] dnsName = {7, 'e', 'x', 'a', 'm', 'p', 'l', 'e', 0, 0, 1, 0, 1};
    assertNull(parse(concat(header(1, 0x0100, 1, 0, 0, 0), dnsName), new Context()));
    // Encoded label with a character outside A-P
    byte[] bad = question("FILESRV", 0x20);
    bad[5] = 'Z';
    assertNull(parse(concat(header(1, 0x0110, 1, 0, 0, 0), bad), new Context()));
    // Implausible counts
    assertNull(parse(concat(header(1, 0x0110, 200, 0, 0, 0), question("A", 0)), new Context()));
    assertNull(parse(concat(header(1, 0x0110, 0, 0, 0, 0), question("A", 0)), new Context()));
  }

  @Test
  public void testTruncatedRecord() {
    byte[] full = concat(header(1, 0x8500, 0, 1, 0, 0),
        nbRecord(name("FILESRV", 0x20), 1, 0, new byte[] {10, 0, 0, 7}));
    try {
      parse(Arrays.copyOf(full, full.length - 2), new Context());
      fail("expected malformed NetBIOS-NS");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("answer 1"));
    }
  }

  @Test
  public void testAddressCap() {
    ByteBuffer rdata = ByteBuffer.allocate(6 * 70);
    for (int i = 0; i < 70; i++) {
      rdata.putShort((short) 0x8000).put(new byte[] {10, 0, 0, (byte) i});
    }
    byte[] r = rdata.array();
    byte[] record = concat(name("GROUP", 0x1C), ByteBuffer.allocate(10).putShort((short) 0x20).putShort((short) 1)
        .putInt(0).putShort((short) r.length).array(), r);
    Context context = new Context();
    NetbiosNsMessage m = parse(concat(header(1, 0x8500, 0, 1, 0, 0), record), context);
    assertEquals(64, m.answers.get(0).addresses.size());
    assertEquals("domain_controllers", m.answers.get(0).suffixName);
    assertEquals(1, context.warnings.size());
  }
}
