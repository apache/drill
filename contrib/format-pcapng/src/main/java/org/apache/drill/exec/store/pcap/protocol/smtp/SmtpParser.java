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
package org.apache.drill.exec.store.pcap.protocol.smtp;

import java.io.ByteArrayOutputStream;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.mail.MailFields;
import org.apache.drill.exec.store.pcap.protocol.mail.MailHeaders;
import org.apache.drill.exec.store.pcap.protocol.mail.MailLines;
import org.apache.drill.exec.store.pcap.protocol.mail.MailMessage;

/**
 * Parses the two directions of an SMTP session. Client lines and server replies are paired in order:
 * every client line outside message data gets one reply, and so does each message body.
 */
public final class SmtpParser {
  private static final Pattern GREETING = Pattern.compile("220([ -].*)?");
  private static final Pattern HELO = Pattern.compile("(?i)(HELO|EHLO)( .*)?");
  private static final Pattern REPLY = Pattern.compile("([2-5][0-9][0-9])(?:([ -])(.*))?");
  private static final Pattern VERB = Pattern.compile("[A-Za-z]{1,16}");

  private final MailLines client;
  private final MailLines server;
  private final DecoderContext context;
  private final SmtpSession session = new SmtpSession();
  private boolean serverDone;
  private boolean clientDone;
  private MailMessage bdat;

  private SmtpParser(byte[] client, byte[] server, DecoderContext context) {
    this.client = new MailLines(client);
    this.server = new MailLines(server);
    this.context = context;
  }

  /**
   * @return the session, or null if the exchange is not SMTP
   * @throws IllegalArgumentException if the client speaks SMTP but the server stream is not SMTP replies
   */
  public static SmtpSession parse(byte[] client, byte[] server, DecoderContext context) {
    String firstServer = MailLines.firstLine(server);
    String firstClient = MailLines.firstLine(client);
    boolean greeting = firstServer != null && GREETING.matcher(firstServer).matches();
    boolean helo = firstClient != null && HELO.matcher(firstClient).matches();
    if (!greeting && !helo) {
      return null;
    }
    if (!greeting && server.length > 0 && (firstServer == null || !REPLY.matcher(firstServer).matches())) {
      throw new IllegalArgumentException("malformed reply at byte 0 of server stream");
    }
    SmtpParser parser = new SmtpParser(client, server, context);
    parser.run(greeting);
    return parser.session;
  }

  private void run(boolean greeting) {
    if (greeting) {
      SmtpSession.Reply reply = reply();
      if (reply != null && !reply.lines.get(0).isEmpty()) {
        session.banner = reply.lines.get(0);
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
        SmtpSession.Reply reply = reply();
        sasl = reply == null ? session.credentials.saslExpectsMore() : reply.code == 334;
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
      SmtpSession.Reply reply = reply();
      switch (verb) {
        case "HELO":
        case "EHLO":
          session.helo = argument.trim().isEmpty() ? null : argument.trim();
          if (verb.equals("EHLO") && reply != null && reply.code == 250) {
            for (int i = 1; i < reply.lines.size(); i++) {
              session.extensions.add(reply.lines.get(i), context);
            }
          }
          break;
        case "AUTH": {
          String[] tokens = argument.trim().split(" +");
          String initial = tokens.length > 1 ? tokens[1] : null;
          session.credentials.startSasl(tokens[0], initial, context);
          if (initial != null && !context.exposeCredentials()) {
            argument = tokens[0] + " ***";
          }
          sasl = reply == null ? session.credentials.saslExpectsMore() : reply.code == 334;
          break;
        }
        case "MAIL":
          if (session.mailFrom == null) {
            session.mailFrom = address(argument, "FROM:");
          }
          break;
        case "RCPT": {
          String to = address(argument, "TO:");
          if (to != null) {
            session.rcptTo.add(to, context);
          }
          break;
        }
        case "DATA":
          if (reply == null || reply.code == 354) {
            data();
          }
          break;
        case "BDAT":
          bdat(argument);
          break;
        case "STARTTLS":
          clientDone = true; // the client waits for the reply, then starts TLS or gives up
          if (reply != null && reply.code == 220) {
            session.tlsStarted = true;
            serverDone = true;
          }
          break;
        default:
          break;
      }
      session.commands.add(new String[] {verb, argument}, context);
    }
    if (bdat != null) {
      addMessage(bdat);
    }
    // Replies the client lines did not account for
    SmtpSession.Reply extra = reply();
    while (extra != null) {
      extra = reply();
    }
  }

  /** Reads the next reply, or returns null at the end of the server stream or after unparseable data. */
  private SmtpSession.Reply reply() {
    if (serverDone) {
      return null;
    }
    SmtpSession.Reply reply = null;
    while (true) {
      int start = server.position();
      String line = server.next();
      if (line == null) {
        serverDone = true;
        return null;
      }
      Matcher m = REPLY.matcher(line);
      int code = m.matches() ? Integer.parseInt(m.group(1)) : -1;
      if (code < 0 || (reply != null && code != reply.code)) {
        context.warn("unparseable reply at byte " + start + " of server stream");
        serverDone = true;
        return null;
      }
      if (reply == null) {
        reply = new SmtpSession.Reply(code);
      }
      reply.lines.add(m.group(3) == null ? "" : m.group(3));
      if (!"-".equals(m.group(2))) {
        reply.text = MailLines.cap(String.join("\n", reply.lines));
        session.replies.add(reply, context);
        return reply;
      }
    }
  }

  /** Reads a DATA body: dot-stuffed lines ending with a line holding only ".". */
  private void data() {
    byte[] d = client.data();
    int bodyStart = client.position();
    ByteArrayOutputStream headers = new ByteArrayOutputStream();
    boolean inHeaders = true;
    long size = 0;
    MailMessage message = new MailMessage();
    while (true) {
      int start = client.position();
      int lf = client.lineEnd();
      if (lf < 0) {
        context.warn("message data truncated at byte " + bodyStart);
        clientDone = true;
        break;
      }
      int contentEnd = lf > start && d[lf - 1] == '\r' ? lf - 1 : lf;
      client.seek(lf + 1);
      if (contentEnd - start == 1 && d[start] == '.') {
        break;
      }
      int from = d[start] == '.' && contentEnd > start ? start + 1 : start;
      size += lf + 1 - from;
      if (inHeaders) {
        if (contentEnd == start) {
          inHeaders = false;
        } else if (headers.size() < MailHeaders.MAX_HEADER_BYTES) {
          headers.write(d, from, lf + 1 - from);
        }
      }
    }
    message.headers = MailHeaders.parse(headers);
    message.size = size;
    addMessage(message);
    if (!clientDone) {
      reply(); // the reply to the message
    }
  }

  /** BDAT size [LAST]: a chunk of exactly size bytes follows the command line. */
  private void bdat(String argument) {
    String[] tokens = argument.trim().split(" +");
    Long size = MailFields.number(tokens[0]);
    int start = client.position();
    if (size == null) {
      context.warn("invalid BDAT size at byte " + start);
      clientDone = true;
      return;
    }
    boolean truncated = size > client.data().length - start;
    int end = truncated ? client.data().length : start + size.intValue();
    if (bdat == null) {
      bdat = new MailMessage();
      bdat.headers = MailHeaders.parse(client.data(), start, end);
      bdat.size = 0L;
    }
    bdat.size += end - start;
    client.seek(end);
    if (truncated) {
      context.warn("message data truncated at byte " + start);
      clientDone = true;
    } else if (tokens.length > 1 && tokens[1].equalsIgnoreCase("LAST")) {
      addMessage(bdat);
      bdat = null;
    }
  }

  private void addMessage(MailMessage message) {
    session.messageCount++;
    session.messages.add(message, context);
  }

  /** The address of "FROM:<a@b> params" or "TO:<a@b>", or null. */
  static String address(String argument, String prefix) {
    String a = argument.trim();
    if (!a.regionMatches(true, 0, prefix, 0, prefix.length())) {
      return null;
    }
    a = a.substring(prefix.length()).trim();
    if (a.startsWith("<")) {
      int close = a.indexOf('>');
      return MailLines.cap(close < 0 ? a.substring(1) : a.substring(1, close));
    }
    int space = a.indexOf(' ');
    return MailLines.cap(space < 0 ? a : a.substring(0, space));
  }
}
