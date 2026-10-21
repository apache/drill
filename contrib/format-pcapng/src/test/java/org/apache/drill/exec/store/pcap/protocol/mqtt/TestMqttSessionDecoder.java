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
package org.apache.drill.exec.store.pcap.protocol.mqtt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

public class TestMqttSessionDecoder extends BaseTest {

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

  private static MqttSession parse(byte[] client, byte[] server, Context context) {
    return MqttSessionDecoder.parseStreams(client, -1, server, -1, context);
  }

  private static byte[] str(String s) {
    byte[] b = s.getBytes(StandardCharsets.UTF_8);
    return concat(new byte[] {(byte) (b.length >> 8), (byte) b.length}, b);
  }

  private static byte[] concat(byte[]... parts) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] p : parts) {
      out.write(p, 0, p.length);
    }
    return out.toByteArray();
  }

  private static byte[] remLen(int value) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    do {
      int b = value % 128;
      value /= 128;
      if (value > 0) {
        b |= 0x80;
      }
      out.write(b);
    } while (value > 0);
    return out.toByteArray();
  }

  private static byte[] packet(int type, byte[] body) {
    return concat(new byte[] {(byte) (type << 4)}, remLen(body.length), body);
  }

  private static byte[] connect(int level, int flags, String clientId, String willTopic,
                                String user, String pass) {
    ByteArrayOutputStream body = new ByteArrayOutputStream();
    byte[] varHeader = concat(str("MQTT"), new byte[] {(byte) level, (byte) flags, 0, 60});
    body.write(varHeader, 0, varHeader.length);
    byte[] id = str(clientId);
    body.write(id, 0, id.length);
    if (willTopic != null) {
      byte[] t = str(willTopic);
      byte[] m = str("bye");
      body.write(t, 0, t.length);
      body.write(m, 0, m.length);
    }
    if (user != null) {
      byte[] u = str(user);
      body.write(u, 0, u.length);
    }
    if (pass != null) {
      byte[] p = str(pass);
      body.write(p, 0, p.length);
    }
    return packet(1, body.toByteArray());
  }

  @Test
  public void testConnect() {
    // username + password + will (0x80 | 0x40 | 0x04)
    byte[] client = concat(
        connect(4, 0xC4, "client-1", "will/topic", "alice", "s3cret"),
        packet(8, concat(new byte[] {0, 1}, str("sensors/#"), new byte[] {0})),
        packet(3, concat(str("home/temp"), "21".getBytes(StandardCharsets.UTF_8))));
    byte[] server = packet(2, new byte[] {0, 0});
    Context context = new Context(false);
    MqttSession s = parse(client, server, context);
    assertEquals(Integer.valueOf(4), s.protocolLevel);
    assertEquals("client-1", s.clientId);
    assertEquals("alice", s.userName);
    assertTrue(s.passwordPresent);
    assertNull(s.password);
    assertEquals("will/topic", s.willTopic);
    assertEquals(Arrays.asList("sensors/#"), s.subscribedTopics);
    assertEquals(Arrays.asList("home/temp"), s.publishedTopics);
    assertEquals(Integer.valueOf(0), s.connectReturnCode);
    assertEquals("Connection Accepted", s.connectResult);
    assertEquals(4, s.packetCount);
    assertTrue(context.warnings.isEmpty());
  }

  @Test
  public void testExposedPassword() {
    byte[] client = connect(4, 0xC0, "c1", null, "alice", "s3cret");
    MqttSession s = parse(client, new byte[0], new Context(true));
    assertEquals("s3cret", s.password);
  }

  @Test
  public void testConnackFailure() {
    byte[] client = connect(4, 0x00, "c1", null, null, null);
    byte[] server = packet(2, new byte[] {0, 5});
    MqttSession s = parse(client, server, new Context(false));
    assertFalse(s.passwordPresent);
    assertEquals(Integer.valueOf(5), s.connectReturnCode);
    assertEquals("Not Authorized", s.connectResult);
  }

  @Test
  public void testNotMqtt() {
    byte[] http = "GET / HTTP/1.0\r\n\r\n".getBytes(StandardCharsets.UTF_8);
    assertNull(parse(http, new byte[0], new Context(false)));
    assertNull(parse(new byte[0], new byte[0], new Context(false)));
  }

  @Test
  public void testTruncatedConnectThrows() {
    byte[] full = connect(4, 0xC0, "client-1", null, "alice", "s3cret");
    // Keep a valid fixed header and MQTT protocol name but cut the payload short
    byte[] cut = Arrays.copyOf(full, 12);
    cut[1] = (byte) (cut.length - 2); // remaining length now points past the buffer content
    try {
      parse(cut, new byte[0], new Context(false));
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().startsWith("truncated CONNECT"));
    }
  }
}
