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

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.TcpSession;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.SessionProtocolDecoder;
import org.apache.drill.exec.store.pcap.protocol.TcpStream;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * MQTT (TCP port 1883): the cleartext control packets of a session. The CONNECT names the client, the
 * protocol level, the user name, and whether a password is set; PUBLISH and SUBSCRIBE carry topics; CONNACK
 * carries the return code. Only the fields that MQTT 3.1/3.1.1/5.0 send in clear are read; payloads are not.
 */
public class MqttSessionDecoder implements SessionProtocolDecoder<MqttSession> {
  static final int PORT = 1883;
  static final int MAX_ITEMS = 64;
  static final int MAX_MESSAGES = 1000;
  static final int MAX_STRING = 4096;

  private static final int CONNECT = 1;
  private static final int CONNACK = 2;
  private static final int PUBLISH = 3;
  private static final int SUBSCRIBE = 8;

  @Override
  public String protocol() {
    return "mqtt";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("protocol_level", MinorType.INT)
        .addNullable("client_id", MinorType.VARCHAR)
        .addNullable("user_name", MinorType.VARCHAR)
        .addNullable("password_present", MinorType.BIT)
        .addNullable("password", MinorType.VARCHAR)
        .addNullable("will_topic", MinorType.VARCHAR)
        .addNullable("connect_return_code", MinorType.INT)
        .addNullable("connect_result", MinorType.VARCHAR)
        .addArray("published_topics", MinorType.VARCHAR)
        .addArray("subscribed_topics", MinorType.VARCHAR)
        .addNullable("packet_count", MinorType.INT);
  }

  @Override
  public boolean accepts(TcpSession session) {
    return session.getSrcPort() == PORT || session.getDstPort() == PORT;
  }

  @Override
  public MqttSession parse(TcpStream fromClient, TcpStream fromServer, DecoderContext context) {
    return parseStreams(fromClient.data(), fromClient.firstGap(), fromServer.data(), fromServer.firstGap(), context);
  }

  /** @return null if the client does not open with an MQTT CONNECT. */
  static MqttSession parseStreams(byte[] client, long clientGap, byte[] server, long serverGap,
                                  DecoderContext context) {
    if (!startsWithConnect(client)) {
      return null;
    }
    MqttSession s = new MqttSession();
    Set<String> capped = new HashSet<>();
    readStream(client, s, capped, clientGap, "client", context);
    readStream(server, s, capped, serverGap, "server", context);
    return s;
  }

  /** The client must open with a CONNECT whose protocol name is MQTT or MQIsdp. */
  private static boolean startsWithConnect(byte[] data) {
    if (data.length < 2 || ((data[0] & 0xFF) >> 4) != CONNECT) {
      return false;
    }
    int[] after = new int[1];
    long remaining = remainingLength(data, 1, after);
    if (remaining < 0) {
      return false;
    }
    String name = readProtocolName(data, after[0], (int) (after[0] + remaining));
    return "MQTT".equals(name) || "MQIsdp".equals(name);
  }

  private static String readProtocolName(byte[] data, int pos, int limit) {
    if (pos + 2 > limit || pos + 2 > data.length) {
      return null;
    }
    int len = u16(data, pos);
    if (len < 0 || pos + 2 + len > limit || pos + 2 + len > data.length) {
      return null;
    }
    return new String(data, pos + 2, len, StandardCharsets.UTF_8);
  }

  /** Walks the control packets of one direction. */
  private static void readStream(byte[] data, MqttSession s, Set<String> capped,
                                 long gap, String direction, DecoderContext context) {
    int pos = 0;
    int count = 0;
    while (pos < data.length) {
      if (count >= MAX_MESSAGES) {
        context.warn("stopped after " + MAX_MESSAGES + " packets");
        return;
      }
      int type = (data[pos] & 0xFF) >> 4;
      int[] after = new int[1];
      long remaining = remainingLength(data, pos + 1, after);
      if (remaining < 0) {
        truncated(gap, direction, context);
        return;
      }
      int body = after[0];
      int end = (int) (body + remaining);
      if (end > data.length) {
        truncated(gap, direction, context);
        return;
      }
      s.packetCount++;
      count++;
      switch (type) {
        case CONNECT:
          parseConnect(data, body, end, s, context);
          break;
        case CONNACK:
          parseConnack(data, body, end, s);
          break;
        case PUBLISH:
          parsePublish(data, body, end, s, capped, context);
          break;
        case SUBSCRIBE:
          parseSubscribe(data, body, end, s, capped, context);
          break;
        default:
          break;
      }
      pos = end;
    }
  }

  /** @throws IllegalArgumentException if the CONNECT is this protocol but malformed. */
  private static void parseConnect(byte[] data, int pos, int end, MqttSession s, DecoderContext context) {
    String name = readProtocolName(data, pos, end);
    if (name == null) {
      throw new IllegalArgumentException("truncated CONNECT protocol name");
    }
    int p = pos + 2 + name.length();
    if (p + 4 > end) {
      throw new IllegalArgumentException("truncated CONNECT variable header");
    }
    s.protocolLevel = data[p] & 0xFF;
    int flags = data[p + 1] & 0xFF;
    boolean hasWill = (flags & 0x04) != 0;
    boolean hasUser = (flags & 0x80) != 0;
    boolean hasPass = (flags & 0x40) != 0;
    p += 4; // protocol level, connect flags, keep alive
    if (s.protocolLevel == 5) {
      // MQTT 5.0 CONNECT properties
      int[] after = new int[1];
      long propLen = remainingLength(data, p, after);
      if (propLen < 0 || after[0] + propLen > end) {
        throw new IllegalArgumentException("truncated CONNECT properties");
      }
      p = (int) (after[0] + propLen);
    }
    int[] np = new int[1];
    s.clientId = readString(data, p, end, "CONNECT client identifier", np);
    p = np[0];
    if (hasWill) {
      if (s.protocolLevel == 5) {
        int[] after = new int[1];
        long propLen = remainingLength(data, p, after);
        if (propLen < 0 || after[0] + propLen > end) {
          throw new IllegalArgumentException("truncated CONNECT will properties");
        }
        p = (int) (after[0] + propLen);
      }
      s.willTopic = cap(readString(data, p, end, "CONNECT will topic", np));
      p = np[0];
      readString(data, p, end, "CONNECT will payload", np); // will message bytes, not exposed
      p = np[0];
    }
    if (hasUser) {
      s.userName = cap(readString(data, p, end, "CONNECT user name", np));
      p = np[0];
    }
    if (hasPass) {
      s.passwordPresent = true;
      String pass = readString(data, p, end, "CONNECT password", np);
      if (context.exposeCredentials()) {
        s.password = cap(pass);
      }
    }
  }

  private static void parseConnack(byte[] data, int pos, int end, MqttSession s) {
    if (pos + 2 > end) {
      return;
    }
    int code = data[pos + 1] & 0xFF;
    s.connectReturnCode = code;
    s.connectResult = returnCodeName(code);
  }

  private static void parsePublish(byte[] data, int pos, int end, MqttSession s,
                                   Set<String> capped, DecoderContext context) {
    int[] np = new int[1];
    String topic = readString(data, pos, end, "PUBLISH topic", np);
    add(s.publishedTopics, cap(topic), "published_topics", capped, context);
  }

  private static void parseSubscribe(byte[] data, int pos, int end, MqttSession s,
                                     Set<String> capped, DecoderContext context) {
    int p = pos + 2; // packet identifier
    if (p > end) {
      throw new IllegalArgumentException("truncated SUBSCRIBE");
    }
    if (s.protocolLevel != null && s.protocolLevel == 5) {
      int[] after = new int[1];
      long propLen = remainingLength(data, p, after);
      if (propLen < 0 || after[0] + propLen > end) {
        throw new IllegalArgumentException("truncated SUBSCRIBE properties");
      }
      p = (int) (after[0] + propLen);
    }
    while (p < end) {
      int[] np = new int[1];
      String filter = readString(data, p, end, "SUBSCRIBE topic filter", np);
      add(s.subscribedTopics, cap(filter), "subscribed_topics", capped, context);
      p = np[0] + 1; // subscription options byte
    }
  }

  /** Reads a two-byte-length-prefixed UTF-8 string. @throws IllegalArgumentException if it overruns. */
  private static String readString(byte[] data, int pos, int end, String what, int[] next) {
    if (pos + 2 > end) {
      throw new IllegalArgumentException("truncated " + what);
    }
    int len = u16(data, pos);
    if (pos + 2 + len > end) {
      throw new IllegalArgumentException("truncated " + what);
    }
    next[0] = pos + 2 + len;
    return new String(data, pos + 2, len, StandardCharsets.UTF_8);
  }

  /**
   * Decodes an MQTT variable-length integer (remaining length).
   *
   * @param next receives the offset just past the encoded integer
   * @return the value, or -1 if the encoding is incomplete within data
   */
  private static long remainingLength(byte[] data, int pos, int[] next) {
    long value = 0;
    int multiplier = 1;
    for (int i = 0; i < 4; i++) {
      if (pos + i >= data.length) {
        return -1;
      }
      int b = data[pos + i] & 0xFF;
      value += (long) (b & 0x7F) * multiplier;
      multiplier *= 128;
      if ((b & 0x80) == 0) {
        next[0] = pos + i + 1;
        return value;
      }
    }
    return -1;
  }

  private static String returnCodeName(int code) {
    switch (code) {
      case 0: return "Connection Accepted";
      case 1: return "Unacceptable Protocol Version";
      case 2: return "Identifier Rejected";
      case 3: return "Server Unavailable";
      case 4: return "Bad Username or Password";
      case 5: return "Not Authorized";
      default: return null;
    }
  }

  private static void truncated(long gap, String direction, DecoderContext context) {
    if (gap >= 0) {
      context.warn("stopped at missing data in " + direction + " stream at byte " + gap);
    } else {
      context.warn("truncated packet in " + direction + " stream");
    }
  }

  private static <T> void add(List<T> list, T item, String name, Set<String> capped, DecoderContext context) {
    if (list.size() < MAX_ITEMS) {
      list.add(item);
    } else if (capped.add(name)) {
      context.warn(name + " truncated to " + MAX_ITEMS);
    }
  }

  private static int u16(byte[] b, int at) {
    return ((b[at] & 0xFF) << 8) | (b[at + 1] & 0xFF);
  }

  private static String cap(String s) {
    if (s == null) {
      return null;
    }
    return s.length() > MAX_STRING ? s.substring(0, MAX_STRING) : s;
  }

  @Override
  public void write(MqttSession s, TupleWriter fields) {
    if (s.protocolLevel != null) {
      fields.scalar("protocol_level").setInt(s.protocolLevel);
    }
    setString(fields, "client_id", s.clientId);
    setString(fields, "user_name", s.userName);
    fields.scalar("password_present").setBoolean(s.passwordPresent);
    setString(fields, "password", s.password);
    setString(fields, "will_topic", s.willTopic);
    if (s.connectReturnCode != null) {
      fields.scalar("connect_return_code").setInt(s.connectReturnCode);
    }
    setString(fields, "connect_result", s.connectResult);
    ArrayWriter published = fields.array("published_topics");
    for (String t : s.publishedTopics) {
      published.scalar().setString(t);
    }
    ArrayWriter subscribed = fields.array("subscribed_topics");
    for (String t : s.subscribedTopics) {
      subscribed.scalar().setString(t);
    }
    fields.scalar("packet_count").setInt(s.packetCount);
  }

  private static void setString(TupleWriter w, String name, String value) {
    if (value != null) {
      w.scalar(name).setString(value);
    }
  }
}
