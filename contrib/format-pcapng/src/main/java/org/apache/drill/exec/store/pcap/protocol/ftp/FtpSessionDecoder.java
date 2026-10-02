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
package org.apache.drill.exec.store.pcap.protocol.ftp;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.TcpSession;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.SessionProtocolDecoder;
import org.apache.drill.exec.store.pcap.protocol.TcpStream;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * The FTP control channel (RFC 959, TCP port 21): login, commands, replies and the transfers they set up.
 * Data channels are separate TCP sessions and are not decoded. After an accepted AUTH TLS the rest of the
 * session is encrypted and is not read.
 */
public class FtpSessionDecoder implements SessionProtocolDecoder<FtpSession> {
  static final int PORT = 21;
  static final int MAX_ITEMS = 64;
  static final int MAX_MESSAGES = 1000;
  static final int MAX_STRING = 4096;
  private static final Set<String> TRANSFERS = new HashSet<>(
      Arrays.asList("RETR", "STOR", "STOU", "APPE", "LIST", "NLST", "MLSD"));
  private static final Pattern PASV = Pattern.compile("(\\d{1,3}),(\\d{1,3}),(\\d{1,3}),(\\d{1,3}),(\\d{1,3}),(\\d{1,3})");
  private static final Pattern EPSV = Pattern.compile("\\((\\p{Graph})\\1\\1(\\d{1,5})\\1\\)");
  private static final Pattern IPV4 = Pattern.compile("\\d{1,3}(\\.\\d{1,3}){3}");
  private static final Pattern IPV6 = Pattern.compile("[0-9A-Fa-f:.]{2,45}");

  @Override
  public String protocol() {
    return "ftp";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("banner", MinorType.VARCHAR)
        .addNullable("username", MinorType.VARCHAR)
        .addNullable("password_present", MinorType.BIT)
        .addNullable("password", MinorType.VARCHAR)
        .addNullable("tls_started", MinorType.BIT)
        .addNullable("system_type", MinorType.VARCHAR)
        .addArray("current_directories", MinorType.VARCHAR)
        .addMapArray("transfers")
          .addNullable("command", MinorType.VARCHAR)
          .addNullable("path", MinorType.VARCHAR)
          .addNullable("reply_code", MinorType.INT)
          .addNullable("data_address", MinorType.VARCHAR)
          .resumeSchema()
        .addMapArray("commands")
          .addNullable("command", MinorType.VARCHAR)
          .addNullable("argument", MinorType.VARCHAR)
          .resumeSchema()
        .addMapArray("replies")
          .addNullable("code", MinorType.INT)
          .addNullable("text", MinorType.VARCHAR)
          .resumeSchema();
  }

  @Override
  public boolean accepts(TcpSession session) {
    return session.getSrcPort() == PORT || session.getDstPort() == PORT;
  }

  @Override
  public FtpSession parse(TcpStream fromClient, TcpStream fromServer, DecoderContext context) {
    FtpSession session = parseStreams(fromClient.data(), fromServer.data(), context);
    if (session == null || session.tlsStarted) {
      return session;
    }
    if (fromClient.hasGaps()) {
      context.warn("stopped at missing data in client stream at byte " + fromClient.firstGap());
    }
    if (fromServer.hasGaps()) {
      context.warn("stopped at missing data in server stream at byte " + fromServer.firstGap());
    }
    return session;
  }

  /**
   * @return null unless the server greets with 220 or the client starts with USER or AUTH
   * @throws IllegalArgumentException if it is FTP but a stream does not start with a command or reply
   */
  static FtpSession parseStreams(byte[] client, byte[] server, DecoderContext context) {
    if (!startsWithGreeting(server) && !startsWithLogin(client)) {
      return null;
    }
    FtpSession s = new FtpSession();
    List<FtpSession.Command> commands = readCommands(client, s, context);
    List<FtpSession.Reply> replies = readReplies(server, s, context);

    Set<String> capped = new HashSet<>();
    for (FtpSession.Command c : commands) {
      FtpSession.Command shown = new FtpSession.Command();
      shown.command = c.command;
      shown.argument = "PASS".equals(c.command) && c.argument != null && !context.exposeCredentials()
          ? "***" : c.argument;
      add(s.commands, shown, "commands", capped, context);
    }
    for (FtpSession.Reply r : replies) {
      add(s.replies, r, "replies", capped, context);
    }

    // Each command gets any number of preliminary (1xx) replies, then one final reply
    int r = 0;
    if (!replies.isEmpty() && (replies.get(0).code == 220 || replies.get(0).code == 120)) {
      while (r < replies.size() && replies.get(r).code < 200) {
        r++;
      }
      if (r < replies.size()) {
        s.banner = replies.get(r++).text;
      }
    }
    String dataAddress = null;
    for (FtpSession.Command c : commands) {
      Integer code = null;
      String text = null;
      while (r < replies.size() && replies.get(r).code < 200) {
        code = replies.get(r++).code;
      }
      if (r < replies.size()) {
        code = replies.get(r).code;
        text = replies.get(r++).text;
      }
      String arg = c.argument;
      switch (c.command) {
        case "USER":
          s.username = arg;
          break;
        case "PASS":
          s.passwordPresent = true;
          if (context.exposeCredentials()) {
            s.password = arg;
          }
          break;
        case "SYST":
          if (code != null && code == 215) {
            s.systemType = text;
          }
          break;
        case "CWD":
        case "XCWD":
          if (arg != null && (code == null || code / 100 == 2)) {
            add(s.currentDirectories, arg, "current_directories", capped, context);
          }
          break;
        case "PWD":
        case "XPWD":
          String dir = code != null && code == 257 ? quoted(text) : null;
          if (dir != null) {
            add(s.currentDirectories, dir, "current_directories", capped, context);
          }
          break;
        case "PORT":
          dataAddress = arg != null ? portAddress(arg) : null;
          break;
        case "EPRT":
          dataAddress = arg != null ? eprtAddress(arg) : null;
          break;
        case "PASV":
          dataAddress = code != null && code == 227 ? pasvAddress(text) : null;
          break;
        case "EPSV":
          dataAddress = code != null && code == 229 ? epsvAddress(text) : null;
          break;
        default:
          if (TRANSFERS.contains(c.command)) {
            FtpSession.Transfer t = new FtpSession.Transfer();
            t.command = c.command;
            t.path = arg;
            t.replyCode = code;
            t.dataAddress = dataAddress;
            add(s.transfers, t, "transfers", capped, context);
          }
      }
    }
    return s;
  }

  /** Adds to a list capped at MAX_ITEMS, warning once per list. */
  private static <T> void add(List<T> list, T item, String name, Set<String> capped, DecoderContext context) {
    if (list.size() < MAX_ITEMS) {
      list.add(item);
    } else if (capped.add(name)) {
      context.warn(name + " truncated to " + MAX_ITEMS);
    }
  }

  private static boolean startsWithGreeting(byte[] server) {
    return server.length >= 4 && server[0] == '2' && server[1] == '2' && server[2] == '0'
        && (server[3] == ' ' || server[3] == '-');
  }

  private static boolean startsWithLogin(byte[] client) {
    if (client.length < 5) {
      return false;
    }
    String verb = new String(client, 0, 4, StandardCharsets.ISO_8859_1).toUpperCase(Locale.ROOT);
    return ("USER".equals(verb) || "AUTH".equals(verb)) && (client[4] == ' ' || client[4] == '\r');
  }

  private static List<FtpSession.Command> readCommands(byte[] data, FtpSession s, DecoderContext context) {
    List<FtpSession.Command> commands = new ArrayList<>();
    LineReader lines = new LineReader(data);
    while (!lines.atEnd() && commands.size() < MAX_MESSAGES) {
      int start = lines.pos;
      String line = lines.next();
      FtpSession.Command c = line == null ? null : command(line);
      if (c == null) {
        if (!commands.isEmpty() && "AUTH".equals(commands.get(commands.size() - 1).command)) {
          // The client began the TLS handshake
          s.tlsStarted = true;
        } else if (commands.isEmpty()) {
          throw new IllegalArgumentException("client stream does not start with a command");
        } else {
          context.warn("unparseable data in client stream at byte " + start);
        }
        return commands;
      }
      commands.add(c);
    }
    if (!lines.atEnd()) {
      context.warn("stopped after " + MAX_MESSAGES + " commands");
    }
    return commands;
  }

  private static FtpSession.Command command(String line) {
    int space = line.indexOf(' ');
    String verb = space >= 0 ? line.substring(0, space) : line;
    if (verb.length() < 3 || verb.length() > 4) {
      return null;
    }
    for (int i = 0; i < verb.length(); i++) {
      char ch = verb.charAt(i);
      if (!(ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z')) {
        return null;
      }
    }
    FtpSession.Command c = new FtpSession.Command();
    c.command = verb.toUpperCase(Locale.ROOT);
    c.argument = space >= 0 && space + 1 < line.length() ? line.substring(space + 1) : null;
    return c;
  }

  private static List<FtpSession.Reply> readReplies(byte[] data, FtpSession s, DecoderContext context) {
    List<FtpSession.Reply> replies = new ArrayList<>();
    LineReader lines = new LineReader(data);
    while (!lines.atEnd() && replies.size() < MAX_MESSAGES) {
      int start = lines.pos;
      String line = lines.next();
      int code = line == null ? -1 : replyCode(line);
      if (code < 0) {
        if (replies.isEmpty()) {
          throw new IllegalArgumentException("server stream does not start with a reply");
        }
        context.warn("unparseable data in server stream at byte " + start);
        return replies;
      }
      StringBuilder text = new StringBuilder(line.length() > 4 ? line.substring(4) : "");
      if (line.length() > 3 && line.charAt(3) == '-') {
        // Multi-line reply: ends at a line starting with the same code and a space
        String last = line.substring(0, 3);
        boolean closed = false;
        while (!lines.atEnd()) {
          String more = lines.next();
          if (more == null) {
            break;
          }
          boolean end = more.startsWith(last + " ") || more.equals(last);
          if (text.length() < MAX_STRING) {
            text.append('\n').append(end ? more.substring(Math.min(4, more.length())) : more);
          }
          if (end) {
            closed = true;
            break;
          }
        }
        if (!closed) {
          context.warn("unterminated multi-line reply in server stream at byte " + start);
          return replies;
        }
      }
      FtpSession.Reply r = new FtpSession.Reply();
      r.code = code;
      r.text = cap(text.toString());
      replies.add(r);
      if (code == 234) {
        // AUTH accepted: TLS follows
        s.tlsStarted = true;
        return replies;
      }
    }
    if (!lines.atEnd()) {
      context.warn("stopped after " + MAX_MESSAGES + " replies");
    }
    return replies;
  }

  /** The three-digit code of a reply line, or -1. */
  private static int replyCode(String line) {
    if (line.length() < 3 || line.charAt(0) < '1' || line.charAt(0) > '5') {
      return -1;
    }
    for (int i = 1; i < 3; i++) {
      if (line.charAt(i) < '0' || line.charAt(i) > '9') {
        return -1;
      }
    }
    if (line.length() > 3 && line.charAt(3) != ' ' && line.charAt(3) != '-') {
      return -1;
    }
    return Integer.parseInt(line.substring(0, 3));
  }

  /** The directory in a 257 reply: "name" with embedded quotes doubled. */
  static String quoted(String text) {
    if (text == null || !text.startsWith("\"")) {
      return null;
    }
    StringBuilder out = new StringBuilder();
    for (int i = 1; i < text.length(); i++) {
      char ch = text.charAt(i);
      if (ch == '"') {
        if (i + 1 < text.length() && text.charAt(i + 1) == '"') {
          out.append('"');
          i++;
        } else {
          return out.toString();
        }
      } else {
        out.append(ch);
      }
    }
    return null;
  }

  /** PORT h1,h2,h3,h4,p1,p2 as ip:port. */
  static String portAddress(String arg) {
    Matcher m = PASV.matcher(arg.trim());
    return m.matches() ? hostPort(m) : null;
  }

  /** The address in a 227 reply, like (h1,h2,h3,h4,p1,p2). */
  static String pasvAddress(String text) {
    if (text == null) {
      return null;
    }
    Matcher m = PASV.matcher(text);
    return m.find() ? hostPort(m) : null;
  }

  private static String hostPort(Matcher m) {
    int[] v = new int[6];
    for (int i = 0; i < 6; i++) {
      v[i] = Integer.parseInt(m.group(i + 1));
      if (v[i] > 255) {
        return null;
      }
    }
    return v[0] + "." + v[1] + "." + v[2] + "." + v[3] + ":" + (v[4] * 256 + v[5]);
  }

  /** EPRT |proto|address|port| (RFC 2428) as ip:port or [ipv6]:port. */
  static String eprtAddress(String arg) {
    if (arg.length() < 2 || arg.charAt(0) != arg.charAt(arg.length() - 1)) {
      return null;
    }
    String[] parts = arg.substring(1, arg.length() - 1).split(Pattern.quote(arg.substring(0, 1)), -1);
    if (parts.length != 3 || !validPort(parts[2])) {
      return null;
    }
    if ("1".equals(parts[0]) && IPV4.matcher(parts[1]).matches()) {
      return parts[1] + ":" + Integer.parseInt(parts[2]);
    }
    if ("2".equals(parts[0]) && IPV6.matcher(parts[1]).matches()) {
      return "[" + parts[1].toLowerCase(Locale.ROOT) + "]:" + Integer.parseInt(parts[2]);
    }
    return null;
  }

  /** The port in a 229 reply, like (|||port|). The host is the server's own address, so only :port is given. */
  static String epsvAddress(String text) {
    if (text == null) {
      return null;
    }
    Matcher m = EPSV.matcher(text);
    return m.find() && validPort(m.group(2)) ? ":" + Integer.parseInt(m.group(2)) : null;
  }

  private static boolean validPort(String s) {
    if (s.isEmpty() || s.length() > 5) {
      return false;
    }
    for (int i = 0; i < s.length(); i++) {
      if (s.charAt(i) < '0' || s.charAt(i) > '9') {
        return false;
      }
    }
    return Integer.parseInt(s) <= 65535;
  }

  private static String cap(String s) {
    return s.length() > MAX_STRING ? s.substring(0, MAX_STRING) : s;
  }

  @Override
  public void write(FtpSession s, TupleWriter fields) {
    setString(fields, "banner", s.banner);
    setString(fields, "username", s.username);
    fields.scalar("password_present").setBoolean(s.passwordPresent);
    setString(fields, "password", s.password);
    fields.scalar("tls_started").setBoolean(s.tlsStarted);
    setString(fields, "system_type", s.systemType);
    ArrayWriter dirs = fields.array("current_directories");
    for (String d : s.currentDirectories) {
      dirs.scalar().setString(d);
    }
    ArrayWriter transfers = fields.array("transfers");
    for (FtpSession.Transfer t : s.transfers) {
      TupleWriter w = transfers.tuple();
      setString(w, "command", t.command);
      setString(w, "path", t.path);
      if (t.replyCode != null) {
        w.scalar("reply_code").setInt(t.replyCode);
      }
      setString(w, "data_address", t.dataAddress);
      transfers.save();
    }
    ArrayWriter commands = fields.array("commands");
    for (FtpSession.Command c : s.commands) {
      TupleWriter w = commands.tuple();
      setString(w, "command", c.command);
      setString(w, "argument", c.argument);
      commands.save();
    }
    ArrayWriter replies = fields.array("replies");
    for (FtpSession.Reply r : s.replies) {
      TupleWriter w = replies.tuple();
      w.scalar("code").setInt(r.code);
      setString(w, "text", r.text);
      replies.save();
    }
  }

  private static void setString(TupleWriter w, String name, String value) {
    if (value != null) {
      w.scalar(name).setString(value);
    }
  }

  /** Splits a stream into lines ended by CRLF or LF. */
  private static final class LineReader {
    private final byte[] b;
    private int pos;

    LineReader(byte[] b) {
      this.b = b;
    }

    boolean atEnd() {
      return pos >= b.length;
    }

    /** The next line, or null (without advancing) if it holds control characters. */
    String next() {
      int end = pos;
      while (end < b.length && b[end] != '\n') {
        end++;
      }
      int textEnd = end > pos && b[end - 1] == '\r' ? end - 1 : end;
      for (int i = pos; i < textEnd; i++) {
        int c = b[i] & 0xFF;
        if ((c < 0x20 && c != '\t') || c == 0x7F) {
          return null;
        }
      }
      String line = new String(b, pos, Math.min(textEnd - pos, MAX_STRING * 4), StandardCharsets.UTF_8);
      pos = end < b.length ? end + 1 : end;
      return cap(line);
    }
  }
}
