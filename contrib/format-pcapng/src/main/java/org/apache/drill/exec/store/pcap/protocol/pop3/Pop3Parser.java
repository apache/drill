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
package org.apache.drill.exec.store.pcap.protocol.pop3;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.mail.MailFields;
import org.apache.drill.exec.store.pcap.protocol.mail.MailHeaders;
import org.apache.drill.exec.store.pcap.protocol.mail.MailLines;
import org.apache.drill.exec.store.pcap.protocol.mail.MailMessage;

/**
 * Parses the two directions of a POP3 session. Commands and responses are paired in order; the
 * command decides whether a +OK response is followed by lines ending with a line holding only ".".
 */
public final class Pop3Parser {
  private static final Pattern STATUS = Pattern.compile("(?i)(\\+OK|-ERR)(?: (.*))?");
  private static final Pattern CONTINUATION = Pattern.compile("\\+(?: (.*))?");
  private static final Pattern CLIENT_START = Pattern.compile("(?i)(USER|APOP|CAPA|STLS|AUTH)( .*)?");
  private static final Pattern VERB = Pattern.compile("[A-Za-z]{1,16}");

  private enum Kind { SINGLE, LINES, MESSAGE }

  private static final class Response {
    String status;
    String text;
    final List<String> lines = new ArrayList<>();
    int lineCount;
    MailMessage message;

    boolean ok() {
      return "+OK".equals(status);
    }
  }

  private final MailLines client;
  private final MailLines server;
  private final DecoderContext context;
  private final Pop3Session session = new Pop3Session();
  private boolean serverDone;
  private boolean clientDone;

  private Pop3Parser(byte[] client, byte[] server, DecoderContext context) {
    this.client = new MailLines(client);
    this.server = new MailLines(server);
    this.context = context;
  }

  /**
   * @return the session, or null if the exchange is not POP3
   * @throws IllegalArgumentException if the client speaks POP3 but the server stream is not POP3 responses
   */
  public static Pop3Session parse(byte[] client, byte[] server, DecoderContext context) {
    String firstServer = MailLines.firstLine(server);
    String firstClient = MailLines.firstLine(client);
    Matcher status = firstServer == null ? null : STATUS.matcher(firstServer);
    boolean greeting = status != null && status.matches() && status.group(1).equalsIgnoreCase("+OK");
    boolean clientStart = firstClient != null && CLIENT_START.matcher(firstClient).matches();
    if (!greeting && !clientStart) {
      return null;
    }
    boolean serverStatus = status != null && status.matches();
    if (!greeting && server.length > 0 && !serverStatus) {
      throw new IllegalArgumentException("malformed response at byte 0 of server stream");
    }
    Pop3Parser parser = new Pop3Parser(client, server, context);
    parser.run(serverStatus);
    return parser.session;
  }

  private void run(boolean greeting) {
    if (greeting) {
      Response r = response(Kind.SINGLE);
      if (r != null && r.ok() && !r.text.isEmpty()) {
        session.banner = r.text;
      }
    }
    boolean sasl = false;
    while (!clientDone) {
      int start = client.position();
      String line = client.next();
      if (line == null) {
        break;
      }
      if (sasl) {
        session.credentials.saslResponse(line, context);
        Response r = response(Kind.SINGLE);
        sasl = r == null ? session.credentials.saslExpectsMore() : "+".equals(r.status);
        continue;
      }
      if (session.commandCount == MailLines.MAX_COMMANDS) {
        context.warn("stopped after " + MailLines.MAX_COMMANDS + " commands");
        break;
      }
      int space = line.indexOf(' ');
      String verb = space < 0 ? line : line.substring(0, space);
      String argument = space < 0 ? "" : line.substring(space + 1);
      if (!VERB.matcher(verb).matches()) {
        context.warn("unparseable command at byte " + start + " of client stream");
        break;
      }
      verb = verb.toUpperCase(Locale.ROOT);
      session.commandCount++;
      String[] tokens = argument.trim().split(" +");
      boolean noArgument = argument.trim().isEmpty();
      Kind kind = Kind.SINGLE;
      if (verb.equals("RETR") || verb.equals("TOP")) {
        kind = Kind.MESSAGE;
      } else if (verb.equals("CAPA") || (noArgument && (verb.equals("LIST") || verb.equals("UIDL")
          || verb.equals("AUTH")))) {
        kind = Kind.LINES;
      }
      Response r = response(kind);
      boolean ok = r != null && r.ok();
      switch (verb) {
        case "USER":
          session.credentials.setUsername(argument);
          break;
        case "PASS":
          session.credentials.setPassword(argument, context);
          if (!context.exposeCredentials()) {
            argument = "***";
          }
          break;
        case "APOP":
          // APOP name digest: the digest is not a password
          session.credentials.mechanism = "APOP";
          session.credentials.setUsername(tokens[0]);
          session.credentials.passwordPresent = true;
          if (tokens.length > 1 && !context.exposeCredentials()) {
            argument = tokens[0] + " ***";
          }
          break;
        case "AUTH":
          if (!noArgument) {
            String initial = tokens.length > 1 ? tokens[1] : null;
            session.credentials.startSasl(tokens[0], initial, context);
            if (initial != null && !context.exposeCredentials()) {
              argument = tokens[0] + " ***";
            }
            sasl = r == null ? session.credentials.saslExpectsMore() : "+".equals(r.status);
          }
          break;
        case "STLS":
          clientDone = true; // the client waits for the response, then starts TLS or gives up
          if (ok) {
            session.tlsStarted = true;
            serverDone = true;
          }
          break;
        case "STAT":
          if (ok) {
            session.messageCount = count(r.text.trim().split(" +")[0]);
          }
          break;
        case "LIST":
          if (ok && noArgument && session.messageCount == null) {
            session.messageCount = r.lineCount;
          }
          break;
        case "CAPA":
          if (ok) {
            session.capabilities.clear();
            for (String capability : r.lines) {
              session.capabilities.add(capability, context);
            }
          }
          break;
        case "RETR":
        case "TOP":
          if (ok && r.message != null) {
            r.message.number = count(tokens[0]);
            session.retrieved.add(r.message, context);
          }
          break;
        default:
          break;
      }
      session.commands.add(new String[] {verb, argument}, context);
    }
    Response extra = response(Kind.SINGLE);
    while (extra != null) {
      extra = response(Kind.SINGLE);
    }
  }

  private static Integer count(String s) {
    Long n = MailFields.number(s);
    return n == null || n > Integer.MAX_VALUE ? null : n.intValue();
  }

  /** Reads the next response, or returns null at the end of the server stream or after unparseable data. */
  private Response response(Kind kind) {
    if (serverDone) {
      return null;
    }
    int start = server.position();
    String line = server.next();
    if (line == null) {
      serverDone = true;
      return null;
    }
    Response r = new Response();
    Matcher status = STATUS.matcher(line);
    Matcher continuation = CONTINUATION.matcher(line);
    if (status.matches()) {
      r.status = status.group(1).toUpperCase(Locale.ROOT);
      r.text = status.group(2) == null ? "" : status.group(2);
    } else if (continuation.matches()) {
      r.status = "+";
      r.text = continuation.group(1) == null ? "" : continuation.group(1);
    } else {
      context.warn("unparseable response at byte " + start + " of server stream");
      serverDone = true;
      return null;
    }
    session.replies.add(new String[] {r.status, r.text}, context);
    if (kind != Kind.SINGLE && r.ok()) {
      multiLine(kind, r);
    }
    return r;
  }

  /** Reads dot-stuffed lines up to a line holding only ".". */
  private void multiLine(Kind kind, Response r) {
    byte[] d = server.data();
    int contentStart = server.position();
    ByteArrayOutputStream headers = new ByteArrayOutputStream();
    boolean inHeaders = true;
    long size = 0;
    while (true) {
      int start = server.position();
      int lf = server.lineEnd();
      if (lf < 0) {
        context.warn("multi-line response truncated at byte " + contentStart + " of server stream");
        serverDone = true;
        break;
      }
      int contentEnd = lf > start && d[lf - 1] == '\r' ? lf - 1 : lf;
      server.seek(lf + 1);
      if (contentEnd - start == 1 && d[start] == '.') {
        break;
      }
      int from = d[start] == '.' && contentEnd > start ? start + 1 : start;
      if (kind == Kind.LINES) {
        r.lineCount++;
        if (r.lines.size() < MailLines.MAX_ITEMS) {
          r.lines.add(MailLines.text(d, from, lf));
        }
        continue;
      }
      size += lf + 1 - from;
      if (inHeaders) {
        if (contentEnd == start) {
          inHeaders = false;
        } else if (headers.size() < MailHeaders.MAX_HEADER_BYTES) {
          headers.write(d, from, lf + 1 - from);
        }
      }
    }
    if (kind == Kind.MESSAGE) {
      r.message = new MailMessage();
      r.message.headers = MailHeaders.parse(headers);
      r.message.size = size;
    }
  }
}
