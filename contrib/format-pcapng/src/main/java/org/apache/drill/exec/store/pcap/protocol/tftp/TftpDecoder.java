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

import java.nio.charset.StandardCharsets;
import java.util.Locale;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.PacketProtocolDecoder;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * TFTP (RFC 1350, options from RFC 2347) over UDP port 69. Only packets to or from port 69 are seen, which
 * in practice means read and write requests and the first reply; the rest of a transfer moves to ephemeral
 * ports on both sides and is not decoded. A request is TFTP only if its file name is printable and its mode
 * is netascii, octet or mail.
 */
public class TftpDecoder implements PacketProtocolDecoder<TftpMessage> {
  static final int MAX_ITEMS = 64;
  private static final int MAX_STRING = 4096;
  // Largest DATA payload: block size 65464 (RFC 2348)
  private static final int MAX_DATA = 65464;
  private static final String[] OPCODES = {null, "RRQ", "WRQ", "DATA", "ACK", "ERROR", "OACK"};
  private static final String[] MODES = {"netascii", "octet", "mail"};

  @Override
  public String protocol() {
    return "tftp";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("opcode", MinorType.VARCHAR)
        .addNullable("filename", MinorType.VARCHAR)
        .addNullable("mode", MinorType.VARCHAR)
        .addNullable("block", MinorType.INT)
        .addNullable("data_length", MinorType.INT)
        .addNullable("error_code", MinorType.INT)
        .addNullable("error_message", MinorType.VARCHAR)
        .addMapArray("options")
          .addNullable("name", MinorType.VARCHAR)
          .addNullable("value", MinorType.VARCHAR)
          .resumeSchema();
  }

  @Override
  public boolean accepts(Packet packet) {
    return packet.isUdpPacket() && (packet.getSrc_port() == 69 || packet.getDst_port() == 69);
  }

  @Override
  public TftpMessage parse(Packet packet, byte[] payload, DecoderContext context) {
    if (payload == null || payload.length < 2 || payload[0] != 0 || payload[1] < 1 || payload[1] >= OPCODES.length) {
      return null;
    }
    int opcode = payload[1];
    TftpMessage m = new TftpMessage();
    m.opcode = OPCODES[opcode];
    switch (opcode) {
      case 1:
      case 2:
        return request(payload, m, context);
      case 3:
        if (payload.length < 4 || payload.length > 4 + MAX_DATA) {
          return null;
        }
        m.block = u16(payload, 2);
        m.dataLength = payload.length - 4;
        return m;
      case 4:
        if (payload.length != 4) {
          return null;
        }
        m.block = u16(payload, 2);
        return m;
      case 5:
        return error(payload, m);
      default:
        // OACK: the first option name must look like one
        int end = terminator(payload, 2);
        if (end < 0 || end == 2 || !printable(payload, 2, end)) {
          return null;
        }
        options(payload, 2, m, context);
        return m;
    }
  }

  private static TftpMessage request(byte[] b, TftpMessage m, DecoderContext context) {
    int nameEnd = terminator(b, 2);
    if (nameEnd <= 2 || !printable(b, 2, nameEnd)) {
      return null;
    }
    int modeEnd = terminator(b, nameEnd + 1);
    if (modeEnd < 0) {
      String rest = new String(b, nameEnd + 1, b.length - nameEnd - 1, StandardCharsets.ISO_8859_1)
          .toLowerCase(Locale.ROOT);
      for (String mode : MODES) {
        if (!rest.isEmpty() && mode.startsWith(rest)) {
          throw new IllegalArgumentException("request truncated in mode");
        }
      }
      return null;
    }
    String mode = new String(b, nameEnd + 1, modeEnd - nameEnd - 1, StandardCharsets.ISO_8859_1)
        .toLowerCase(Locale.ROOT);
    if (!isMode(mode)) {
      return null;
    }
    m.filename = string(b, 2, nameEnd);
    m.mode = mode;
    options(b, modeEnd + 1, m, context);
    return m;
  }

  private static boolean isMode(String mode) {
    for (String valid : MODES) {
      if (valid.equals(mode)) {
        return true;
      }
    }
    return false;
  }

  private static TftpMessage error(byte[] b, TftpMessage m) {
    // Codes 0-7 are from RFC 1350, 8 (option negotiation failed) from RFC 2347
    if (b.length < 4 || b[2] != 0 || b[3] > 8 || b[3] < 0) {
      return null;
    }
    m.errorCode = (int) b[3];
    int end = terminator(b, 4);
    if (end < 0) {
      throw new IllegalArgumentException("unterminated error message");
    }
    m.errorMessage = end == 4 ? null : string(b, 4, end);
    return m;
  }

  /** Reads name and value pairs from pos to the end of the payload. */
  private static void options(byte[] b, int pos, TftpMessage m, DecoderContext context) {
    int count = 0;
    while (pos < b.length) {
      int nameEnd = terminator(b, pos);
      if (nameEnd < 0) {
        throw new IllegalArgumentException("unterminated option name");
      }
      if (nameEnd == pos) {
        // Padding after the last option
        return;
      }
      String name = string(b, pos, nameEnd);
      if (nameEnd + 1 >= b.length) {
        throw new IllegalArgumentException("option " + name + " has no value");
      }
      int valueEnd = terminator(b, nameEnd + 1);
      if (valueEnd < 0) {
        throw new IllegalArgumentException("unterminated option value");
      }
      if (count++ < MAX_ITEMS) {
        m.options.add(new String[] {name, string(b, nameEnd + 1, valueEnd)});
      } else if (count == MAX_ITEMS + 1) {
        context.warn("options truncated to " + MAX_ITEMS);
      }
      pos = valueEnd + 1;
    }
  }

  /** Position of the NUL at or after from, or -1. */
  private static int terminator(byte[] b, int from) {
    for (int i = from; i < b.length; i++) {
      if (b[i] == 0) {
        return i;
      }
    }
    return -1;
  }

  /** True if [from, to) has no ASCII control characters. */
  private static boolean printable(byte[] b, int from, int to) {
    for (int i = from; i < to; i++) {
      if ((b[i] & 0xFF) < 0x20 || b[i] == 0x7F) {
        return false;
      }
    }
    return true;
  }

  private static String string(byte[] b, int from, int to) {
    String s = new String(b, from, to - from, StandardCharsets.UTF_8);
    return s.length() > MAX_STRING ? s.substring(0, MAX_STRING) : s;
  }

  private static int u16(byte[] b, int at) {
    return ((b[at] & 0xFF) << 8) | (b[at + 1] & 0xFF);
  }

  @Override
  public void write(TftpMessage m, TupleWriter fields) {
    set(fields, "opcode", m.opcode);
    set(fields, "filename", m.filename);
    set(fields, "mode", m.mode);
    setInt(fields, "block", m.block);
    setInt(fields, "data_length", m.dataLength);
    setInt(fields, "error_code", m.errorCode);
    set(fields, "error_message", m.errorMessage);
    ArrayWriter options = fields.array("options");
    for (String[] o : m.options) {
      TupleWriter t = options.tuple();
      t.scalar("name").setString(o[0]);
      t.scalar("value").setString(o[1]);
      options.save();
    }
  }

  private static void set(TupleWriter fields, String name, String value) {
    if (value != null) {
      fields.scalar(name).setString(value);
    }
  }

  private static void setInt(TupleWriter fields, String name, Integer value) {
    if (value != null) {
      fields.scalar(name).setInt(value);
    }
  }
}
