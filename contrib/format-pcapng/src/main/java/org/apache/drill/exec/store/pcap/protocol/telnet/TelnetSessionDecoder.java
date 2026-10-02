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
package org.apache.drill.exec.store.pcap.protocol.telnet;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.TcpSession;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.SessionProtocolDecoder;
import org.apache.drill.exec.store.pcap.protocol.TcpStream;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * Telnet (TCP port 23). Telnet interleaves IAC command sequences with the data stream. This decoder strips
 * the IAC negotiation and exposes the readable text of each direction, the options negotiated, and the
 * terminal type. It is best-effort text extraction: the login name is the client text sent after a
 * {@code login:}/{@code Username:} prompt, and the password is the client input after a {@code Password:}
 * prompt (Telnet suppresses the echo of a typed password, so only the client's own bytes are read).
 */
public class TelnetSessionDecoder implements SessionProtocolDecoder<TelnetSession> {
  static final int PORT = 23;
  static final int MAX_ITEMS = 64;
  static final int MAX_STRING = 4096;

  private static final int IAC = 255;
  private static final int SE = 240;
  private static final int SB = 250;
  private static final int WILL = 251;
  private static final int WONT = 252;
  private static final int DO = 253;
  private static final int DONT = 254;
  private static final int TERMINAL_TYPE = 24;

  @Override
  public String protocol() {
    return "telnet";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("client_text", MinorType.VARCHAR)
        .addNullable("server_text", MinorType.VARCHAR)
        .addNullable("terminal_type", MinorType.VARCHAR)
        .addNullable("login_name", MinorType.VARCHAR)
        .addNullable("password_present", MinorType.BIT)
        .addNullable("password", MinorType.VARCHAR)
        .addMapArray("options")
          .addNullable("option", MinorType.VARCHAR)
          .addNullable("negotiation", MinorType.VARCHAR)
          .resumeSchema();
  }

  @Override
  public boolean accepts(TcpSession session) {
    return session.getSrcPort() == PORT || session.getDstPort() == PORT;
  }

  @Override
  public TelnetSession parse(TcpStream fromClient, TcpStream fromServer, DecoderContext context) {
    return parseStreams(fromClient.data(), fromServer.data(), context);
  }

  /** @return null if neither direction shows IAC negotiation nor a login prompt. */
  static TelnetSession parseStreams(byte[] client, byte[] server, DecoderContext context) {
    TelnetSession s = new TelnetSession();
    boolean[] sawIac = new boolean[1];
    s.clientText = process(client, s, sawIac, "client", context);
    s.serverText = process(server, s, sawIac, "server", context);
    boolean loginText = hasLoginPrompt(s.serverText) || hasLoginPrompt(s.clientText);
    if (!sawIac[0] && !loginText) {
      return null;
    }
    extractCredentials(s, context);
    return s;
  }

  /**
   * Removes IAC sequences from one direction, recording options and the terminal type, and returns the
   * readable text (capped at {@link #MAX_STRING}).
   */
  private static String process(byte[] data, TelnetSession s, boolean[] sawIac,
                         String direction, DecoderContext context) {
    StringBuilder text = new StringBuilder();
    int i = 0;
    while (i < data.length) {
      int b = data[i] & 0xFF;
      if (b != IAC) {
        if (text.length() < MAX_STRING && (b == '\t' || b == '\r' || b == '\n' || (b >= 0x20 && b <= 0x7E))) {
          text.append((char) b);
        }
        i++;
        continue;
      }
      sawIac[0] = true;
      if (i + 1 >= data.length) {
        break; // IAC at end of stream
      }
      int command = data[i + 1] & 0xFF;
      if (command == IAC) {
        i += 2; // escaped literal 0xFF, not readable text
      } else if (command == WILL || command == WONT || command == DO || command == DONT) {
        if (i + 2 >= data.length) {
          break;
        }
        addOption(s, command, data[i + 2] & 0xFF, context);
        i += 3;
      } else if (command == SB) {
        int end = subnegotiation(data, i + 2, s, direction, context);
        if (end < 0) {
          break; // unterminated subnegotiation
        }
        i = end;
      } else {
        i += 2; // two-byte command (NOP, GA, ...)
      }
    }
    return text.toString();
  }

  /**
   * Reads a subnegotiation starting at {@code start} (the option byte), capturing a terminal type.
   *
   * @return the offset just past the terminating IAC SE, or -1 if it is unterminated
   */
  private static int subnegotiation(byte[] data, int start, TelnetSession s, String direction, DecoderContext context) {
    if (start >= data.length) {
      context.warn("unterminated subnegotiation in " + direction + " stream");
      return -1;
    }
    int option = data[start] & 0xFF;
    int i = start + 1;
    List<Byte> payload = new ArrayList<>();
    while (i < data.length) {
      int b = data[i] & 0xFF;
      if (b == IAC) {
        if (i + 1 < data.length && (data[i + 1] & 0xFF) == SE) {
          captureTerminalType(option, payload, s);
          return i + 2;
        }
        if (i + 1 < data.length && (data[i + 1] & 0xFF) == IAC) {
          payload.add((byte) IAC);
          i += 2;
          continue;
        }
      }
      if (payload.size() < MAX_STRING) {
        payload.add(data[i]);
      }
      i++;
    }
    context.warn("unterminated subnegotiation in " + direction + " stream");
    return -1;
  }

  private static void captureTerminalType(int option, List<Byte> payload, TelnetSession s) {
    // TERMINAL-TYPE IS <name>: sub-command 0 is IS
    if (option != TERMINAL_TYPE || payload.isEmpty() || payload.get(0) != 0) {
      return;
    }
    byte[] bytes = new byte[payload.size() - 1];
    for (int j = 1; j < payload.size(); j++) {
      bytes[j - 1] = payload.get(j);
    }
    if (s.terminalType == null) {
      s.terminalType = cap(new String(bytes, StandardCharsets.US_ASCII));
    }
  }

  private static void addOption(TelnetSession s, int command, int option, DecoderContext context) {
    if (s.options.size() >= MAX_ITEMS) {
      return;
    }
    s.options.add(new TelnetSession.Option(optionName(option), negotiationName(command)));
    if (s.options.size() == MAX_ITEMS) {
      context.warn("options truncated to " + MAX_ITEMS);
    }
  }

  private static String negotiationName(int command) {
    switch (command) {
      case WILL: return "WILL";
      case WONT: return "WONT";
      case DO: return "DO";
      case DONT: return "DONT";
      default: return "UNKNOWN";
    }
  }

  private static String optionName(int option) {
    switch (option) {
      case 0: return "BINARY";
      case 1: return "ECHO";
      case 3: return "SUPPRESS_GO_AHEAD";
      case 5: return "STATUS";
      case 6: return "TIMING_MARK";
      case 24: return "TERMINAL_TYPE";
      case 31: return "NAWS";
      case 32: return "TERMINAL_SPEED";
      case 33: return "REMOTE_FLOW_CONTROL";
      case 34: return "LINEMODE";
      case 35: return "X_DISPLAY_LOCATION";
      case 36: return "OLD_ENVIRON";
      case 37: return "AUTHENTICATION";
      case 38: return "ENCRYPTION";
      case 39: return "NEW_ENVIRON";
      default: return "OPTION_" + option;
    }
  }

  private static boolean hasLoginPrompt(String text) {
    if (text == null) {
      return false;
    }
    String lower = text.toLowerCase(Locale.ROOT);
    return lower.contains("login:") || lower.contains("username:") || lower.contains("password:");
  }

  /** Best-effort: the first client line is the login name, the next client line the password. */
  private static void extractCredentials(TelnetSession s, DecoderContext context) {
    String serverLower = s.serverText == null ? "" : s.serverText.toLowerCase(Locale.ROOT);
    boolean wantsLogin = serverLower.contains("login:") || serverLower.contains("username:");
    boolean wantsPassword = serverLower.contains("password:");
    if (!wantsLogin && !wantsPassword) {
      return;
    }
    List<String> lines = clientLines(s.clientText);
    int idx = 0;
    if (wantsLogin && idx < lines.size()) {
      s.loginName = cap(lines.get(idx++));
    }
    if (wantsPassword) {
      s.passwordPresent = true;
      if (idx < lines.size() && context.exposeCredentials()) {
        s.password = cap(lines.get(idx));
      }
    }
  }

  private static List<String> clientLines(String text) {
    List<String> lines = new ArrayList<>();
    if (text == null) {
      return lines;
    }
    for (String raw : text.split("\n", -1)) {
      String line = raw.replace("\r", "").trim();
      if (!line.isEmpty()) {
        lines.add(line);
      }
    }
    return lines;
  }

  private static String cap(String s) {
    if (s == null) {
      return null;
    }
    return s.length() > MAX_STRING ? s.substring(0, MAX_STRING) : s;
  }

  @Override
  public void write(TelnetSession s, TupleWriter fields) {
    setString(fields, "client_text", s.clientText);
    setString(fields, "server_text", s.serverText);
    setString(fields, "terminal_type", s.terminalType);
    setString(fields, "login_name", s.loginName);
    fields.scalar("password_present").setBoolean(s.passwordPresent);
    setString(fields, "password", s.password);
    ArrayWriter options = fields.array("options");
    for (TelnetSession.Option o : s.options) {
      TupleWriter w = options.tuple();
      setString(w, "option", o.option);
      setString(w, "negotiation", o.negotiation);
      options.save();
    }
  }

  private static void setString(TupleWriter w, String name, String value) {
    if (value != null) {
      w.scalar(name).setString(value);
    }
  }
}
