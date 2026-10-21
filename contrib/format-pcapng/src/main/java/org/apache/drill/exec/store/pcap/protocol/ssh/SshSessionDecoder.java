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
package org.apache.drill.exec.store.pcap.protocol.ssh;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.TcpSession;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.SessionProtocolDecoder;
import org.apache.drill.exec.store.pcap.protocol.TcpStream;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * The cleartext start of an SSH-2 connection (RFC 4253) on TCP ports 22 and 2222: each side's identification
 * string and first SSH_MSG_KEXINIT, with the HASSH fingerprints. Everything after KEXINIT is encrypted
 * and is not read.
 */
public class SshSessionDecoder implements SessionProtocolDecoder<SshSession> {
  static final int MAX_ITEMS = 64;
  static final int MAX_STRING = 4096;
  /** RFC 4253 section 4.2: the identification line including CR LF. */
  private static final int MAX_IDENTIFICATION = 255;
  /** Lines a server may send before its identification. */
  private static final int MAX_PRE_LINES = 16;
  private static final int MAX_PRE_BYTES = 8192;
  /** Larger than the 35000 bytes RFC 4253 requires, as OpenSSH accepts. */
  private static final int MAX_PACKET = 256 * 1024;
  private static final int KEXINIT = 20;
  private static final String[] SIDES = {"client", "server"};
  private static final String[] LISTS = {"kex_algorithms", "host_key_algorithms", "ciphers", "macs", "compression"};

  @Override
  public String protocol() {
    return "ssh";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    for (String side : SIDES) {
      fields.addNullable(side + "_version", MinorType.VARCHAR)
          .addNullable(side + "_software", MinorType.VARCHAR)
          .addNullable(side + "_comments", MinorType.VARCHAR);
      for (String list : LISTS) {
        fields.addArray(side + "_" + list, MinorType.VARCHAR);
      }
    }
    fields.addNullable("hassh", MinorType.VARCHAR)
        .addNullable("hassh_string", MinorType.VARCHAR)
        .addNullable("hassh_server", MinorType.VARCHAR)
        .addNullable("hassh_server_string", MinorType.VARCHAR);
  }

  @Override
  public boolean accepts(TcpSession session) {
    return isSshPort(session.getSrcPort()) || isSshPort(session.getDstPort());
  }

  private static boolean isSshPort(int port) {
    return port == 22 || port == 2222;
  }

  @Override
  public SshSession parse(TcpStream fromClient, TcpStream fromServer, DecoderContext context) {
    return parseStreams(fromClient.data(), fromClient.firstGap(), fromServer.data(), fromServer.firstGap(), context);
  }

  /**
   * @param clientGap offset of the first missing byte in the client stream, or -1
   * @return null if neither side sends an SSH-2.0 or SSH-1.99 identification string
   */
  static SshSession parseStreams(byte[] client, long clientGap, byte[] server, long serverGap,
                                 DecoderContext context) {
    int[] clientEnd = new int[1];
    int[] serverEnd = new int[1];
    SshSession s = new SshSession();
    s.client = identification(client, false, clientEnd);
    s.server = identification(server, true, serverEnd);
    if (s.client == null && s.server == null) {
      return null;
    }
    kexinit(s.client, client, clientEnd[0], clientGap, "client", context);
    kexinit(s.server, server, serverEnd[0], serverGap, "server", context);
    return s;
  }

  /**
   * Reads the identification string. A server may send other lines first; a client may not.
   *
   * @param end receives the offset just past the identification line
   * @return null if there is no SSH-2.0 or SSH-1.99 identification
   */
  private static SshSession.Side identification(byte[] data, boolean server, int[] end) {
    int start = 0;
    String direction = server ? "server" : "client";
    for (int lines = 0; ; lines++) {
      if (startsWith(data, start, "SSH-")) {
        break;
      }
      if (!server || lines == MAX_PRE_LINES) {
        return null;
      }
      int lf = indexOf(data, start, Math.min(data.length, MAX_PRE_BYTES), (byte) '\n');
      if (lf < 0) {
        return null;
      }
      for (int i = start; i < lf; i++) {
        int c = data[i] & 0xFF;
        if (c < 0x20 && c != '\t' && c != '\r') {
          return null;
        }
      }
      start = lf + 1;
    }
    if (!startsWith(data, start, "SSH-2.0-") && !startsWith(data, start, "SSH-1.99-")) {
      return null;
    }
    int lf = indexOf(data, start, Math.min(data.length, start + MAX_IDENTIFICATION), (byte) '\n');
    if (lf < 0) {
      if (data.length - start < MAX_IDENTIFICATION) {
        throw new IllegalArgumentException("truncated " + direction + " identification string");
      }
      throw new IllegalArgumentException(direction + " identification string longer than 255 bytes");
    }
    end[0] = lf + 1;
    int textEnd = lf > start && data[lf - 1] == '\r' ? lf - 1 : lf;
    String line = new String(data, start, textEnd - start, StandardCharsets.ISO_8859_1);
    for (int i = 0; i < line.length(); i++) {
      if (line.charAt(i) < 0x20 || line.charAt(i) > 0x7E) {
        throw new IllegalArgumentException("malformed " + direction + " identification string");
      }
    }
    SshSession.Side side = new SshSession.Side();
    int dash = line.indexOf('-', 4);
    side.version = line.substring(4, dash);
    String rest = line.substring(dash + 1);
    int space = rest.indexOf(' ');
    side.software = space >= 0 ? rest.substring(0, space) : rest;
    side.comments = space >= 0 && space + 1 < rest.length() ? rest.substring(space + 1) : null;
    if (side.software.isEmpty()) {
      throw new IllegalArgumentException("malformed " + direction + " identification string");
    }
    return side;
  }

  /** Reads the KEXINIT binary packet at offset into side. Problems are warnings: the identification stands. */
  private static void kexinit(SshSession.Side side, byte[] data, int offset, long gap, String direction,
                              DecoderContext context) {
    if (side == null) {
      if (data.length > 0) {
        context.warn("no identification string in " + direction + " stream");
      }
      return;
    }
    if (offset == data.length && gap < 0) {
      return; // Nothing more was sent
    }
    if (data.length - offset < 6) {
      truncated(direction, gap, context);
      return;
    }
    long packetLength = u32(data, offset);
    int padding = data[offset + 4] & 0xFF;
    if (packetLength > MAX_PACKET || padding < 4 || padding + 2 > packetLength) {
      context.warn("invalid binary packet in " + direction + " stream at byte " + offset);
      return;
    }
    if (packetLength > data.length - offset - 4) {
      truncated(direction, gap, context);
      return;
    }
    int p = offset + 5;
    int payloadEnd = p + (int) packetLength - padding - 1;
    int type = data[p] & 0xFF;
    if (type != KEXINIT) {
      context.warn("first packet in " + direction + " stream is type " + type + ", not KEXINIT");
      return;
    }
    p += 1 + 16; // message type and cookie
    String[] lists = new String[10];
    for (int i = 0; i < lists.length; i++) {
      if (p + 4 > payloadEnd || u32(data, p) > payloadEnd - p - 4) {
        context.warn("malformed KEXINIT in " + direction + " stream: name-list " + (i + 1) + " overruns the packet");
        return;
      }
      int length = (int) u32(data, p);
      lists[i] = new String(data, p + 4, length, StandardCharsets.ISO_8859_1);
      p += 4 + length;
    }
    // Lists 2-9 come in pairs: client-to-server, then server-to-client
    int own = "client".equals(direction) ? 0 : 1;
    String kex = lists[0];
    String ciphers = lists[2 + own];
    String macs = lists[4 + own];
    String compression = lists[6 + own];
    side.kexAlgorithms = names(kex, direction, "kex_algorithms", context);
    side.hostKeyAlgorithms = names(lists[1], direction, "host_key_algorithms", context);
    side.ciphers = names(ciphers, direction, "ciphers", context);
    side.macs = names(macs, direction, "macs", context);
    side.compression = names(compression, direction, "compression", context);
    // HASSH (github.com/salesforce/hassh): MD5 of kex;encryption;mac;compression for the sender's direction
    String hassh = kex + ";" + ciphers + ";" + macs + ";" + compression;
    side.hassh = md5(hassh);
    if (hassh.length() > MAX_STRING) {
      context.warn(direction + " hassh_string truncated to " + MAX_STRING + " characters");
      hassh = hassh.substring(0, MAX_STRING);
    }
    side.hasshString = hassh;
  }

  private static void truncated(String direction, long gap, DecoderContext context) {
    if (gap >= 0) {
      context.warn("stopped at missing data in " + direction + " stream at byte " + gap);
    } else {
      context.warn("truncated KEXINIT in " + direction + " stream");
    }
  }

  private static List<String> names(String list, String direction, String field, DecoderContext context) {
    List<String> names = new ArrayList<>();
    if (list.isEmpty()) {
      return names;
    }
    int start = 0;
    while (start <= list.length()) {
      if (names.size() == MAX_ITEMS) {
        context.warn(direction + "_" + field + " truncated to " + MAX_ITEMS);
        break;
      }
      int comma = list.indexOf(',', start);
      int end = comma >= 0 ? comma : list.length();
      String name = list.substring(start, end);
      names.add(name.length() > MAX_STRING ? name.substring(0, MAX_STRING) : name);
      start = end + 1;
    }
    return names;
  }

  private static String md5(String s) {
    try {
      byte[] digest = MessageDigest.getInstance("MD5").digest(s.getBytes(StandardCharsets.ISO_8859_1));
      StringBuilder hex = new StringBuilder();
      for (byte b : digest) {
        hex.append(String.format("%02x", b));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private static boolean startsWith(byte[] data, int at, String prefix) {
    if (data.length - at < prefix.length()) {
      return false;
    }
    for (int i = 0; i < prefix.length(); i++) {
      if (data[at + i] != prefix.charAt(i)) {
        return false;
      }
    }
    return true;
  }

  private static int indexOf(byte[] data, int from, int to, byte value) {
    for (int i = from; i < to; i++) {
      if (data[i] == value) {
        return i;
      }
    }
    return -1;
  }

  private static long u32(byte[] b, int at) {
    return ((b[at] & 0xFFL) << 24) | ((b[at + 1] & 0xFF) << 16) | ((b[at + 2] & 0xFF) << 8) | (b[at + 3] & 0xFF);
  }

  @Override
  public void write(SshSession s, TupleWriter fields) {
    SshSession.Side[] sides = {s.client, s.server};
    for (int i = 0; i < sides.length; i++) {
      SshSession.Side side = sides[i];
      if (side == null) {
        continue;
      }
      String prefix = SIDES[i] + "_";
      setString(fields, prefix + "version", side.version);
      setString(fields, prefix + "software", side.software);
      setString(fields, prefix + "comments", side.comments);
      List<?>[] lists = {side.kexAlgorithms, side.hostKeyAlgorithms, side.ciphers, side.macs, side.compression};
      for (int l = 0; l < LISTS.length; l++) {
        if (lists[l] == null) {
          continue;
        }
        ArrayWriter array = fields.array(prefix + LISTS[l]);
        for (Object name : lists[l]) {
          array.scalar().setString((String) name);
        }
      }
    }
    if (s.client != null) {
      setString(fields, "hassh", s.client.hassh);
      setString(fields, "hassh_string", s.client.hasshString);
    }
    if (s.server != null) {
      setString(fields, "hassh_server", s.server.hassh);
      setString(fields, "hassh_server_string", s.server.hasshString);
    }
  }

  private static void setString(TupleWriter w, String name, String value) {
    if (value != null) {
      w.scalar(name).setString(value);
    }
  }
}
