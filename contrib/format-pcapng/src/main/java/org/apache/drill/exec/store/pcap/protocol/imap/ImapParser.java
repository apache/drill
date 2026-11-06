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
package org.apache.drill.exec.store.pcap.protocol.imap;

import java.nio.charset.StandardCharsets;
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
 * Parses the two directions of an IMAP session. Client commands are matched to the server's tagged
 * completions by tag; the server stream is read up to a STARTTLS completion before the client stream
 * goes on, so neither side is read past the switch to TLS.
 */
public final class ImapParser {
  private static final String TAG_CHARS = "[\\x21-\\x7E&&[^(){%*\"\\\\\\]+]]{1,64}";
  private static final Pattern TAG = Pattern.compile(TAG_CHARS);
  private static final Pattern GREETING = Pattern.compile("(?i)\\* (OK|PREAUTH|BYE)( .*)?");
  private static final Pattern CLIENT_START = Pattern.compile(
      "(?i)" + TAG_CHARS + " (CAPABILITY|LOGIN|AUTHENTICATE|STARTTLS|NOOP|ID|LOGOUT)( .*)?");
  private static final Pattern SERVER_LINE = Pattern.compile("(\\*|\\+|" + TAG_CHARS + ")( .*)?");
  private static final Pattern COMMAND = Pattern.compile("[A-Za-z]{1,32}");
  private static final Pattern CAPABILITY_CODE = Pattern.compile("(?i)\\[CAPABILITY ([^\\]]*)\\]");

  private final ImapReader client;
  private final ImapReader server;
  private final DecoderContext context;
  private final ImapSession session = new ImapSession();
  /** tag, mailbox, status of SELECT and EXAMINE commands */
  private final List<String[]> selects = new ArrayList<>();
  private boolean serverDone;
  private boolean clientDone;
  /** Tag of the last tagged completion read. */
  private String lastTag;

  private ImapParser(byte[] client, byte[] server, DecoderContext context) {
    this.client = new ImapReader(client);
    this.server = new ImapReader(server);
    this.context = context;
  }

  /**
   * @return the session, or null if the exchange is not IMAP
   * @throws IllegalArgumentException if the client speaks IMAP but the server stream is not IMAP responses
   */
  public static ImapSession parse(byte[] client, byte[] server, DecoderContext context) {
    String firstServer = MailLines.firstLine(server);
    String firstClient = MailLines.firstLine(client);
    boolean greeting = firstServer != null && GREETING.matcher(firstServer).matches();
    boolean clientStart = firstClient != null && CLIENT_START.matcher(firstClient).matches();
    if (!greeting && !clientStart) {
      return null;
    }
    if (!greeting && server.length > 0 && (firstServer == null || !SERVER_LINE.matcher(firstServer).matches())) {
      throw new IllegalArgumentException("malformed response at byte 0 of server stream");
    }
    ImapParser parser = new ImapParser(client, server, context);
    parser.run();
    return parser.session;
  }

  private void run() {
    boolean sasl = false;
    while (!clientDone && !client.atEnd()) {
      int start = client.position();
      int lf = client.lineEnd();
      if (lf < 0) {
        break;
      }
      String line = MailLines.text(client.data(), start, lf);
      try {
        if (sasl && line.indexOf(' ') < 0) {
          // A SASL response; a tagged command always has a space
          session.credentials.saslResponse(client.restOfLine(), context);
          continue;
        }
        sasl = false;
        if (line.equalsIgnoreCase("DONE")) {
          client.restOfLine(); // ends IDLE
          continue;
        }
        if (session.commandCount == MailLines.MAX_COMMANDS) {
          context.warn("stopped after " + MailLines.MAX_COMMANDS + " commands");
          break;
        }
        sasl = command(start);
      } catch (ImapReader.Problem p) {
        problem(p, start, "client", "command");
        clientDone = true;
      }
    }
    if (!serverDone) {
      readServer(null);
    }
    for (String[] select : selects) {
      if (select[2] == null || select[2].equals("OK")) {
        session.selectedMailboxes.add(select[1], context);
      }
    }
  }

  /** Reads one tagged command; returns true if SASL responses may follow. */
  private boolean command(int start) {
    String tag = client.token();
    String command = client.token();
    if (tag == null || command == null || !TAG.matcher(tag).matches() || !COMMAND.matcher(command).matches()) {
      context.warn("unparseable command at byte " + start + " of client stream");
      clientDone = true;
      return false;
    }
    command = command.toUpperCase(Locale.ROOT);
    session.commandCount++;
    boolean sasl = false;
    String argument;
    switch (command) {
      case "LOGIN": {
        List<Object> values = client.values();
        String user = values.size() > 0 ? ImapReader.string(values.get(0)) : null;
        String pass = values.size() > 1 ? ImapReader.string(values.get(1)) : null;
        if (user != null) {
          session.credentials.setUsername(user);
        }
        if (pass != null) {
          session.credentials.setPassword(pass, context);
        }
        if (context.exposeCredentials() || values.size() < 2) {
          argument = ImapReader.render(values);
        } else {
          argument = ImapReader.render(values.subList(0, 1)) + " ***";
        }
        break;
      }
      case "AUTHENTICATE": {
        List<Object> values = client.values();
        String mechanism = values.size() > 0 ? ImapReader.string(values.get(0)) : null;
        String initial = values.size() > 1 ? ImapReader.string(values.get(1)) : null;
        if (mechanism != null) {
          session.credentials.startSasl(mechanism, initial, context);
          sasl = true;
        }
        if (initial != null && !context.exposeCredentials()) {
          argument = ImapReader.render(values.subList(0, 1)) + " ***";
        } else {
          argument = ImapReader.render(values);
        }
        break;
      }
      case "SELECT":
      case "EXAMINE": {
        List<Object> values = client.values();
        String mailbox = values.size() > 0 ? ImapReader.string(values.get(0)) : null;
        if (mailbox != null && selects.size() < MailLines.MAX_ITEMS) {
          selects.add(new String[] {tag, mailbox, null});
        }
        argument = ImapReader.render(values);
        break;
      }
      case "STARTTLS": {
        argument = client.restOfLine();
        session.commands.add(new String[] {tag, command, argument}, context);
        // The client waits for the completion, then starts TLS or carries on
        String status = readServer(tag);
        if ("OK".equals(status)) {
          session.tlsStarted = true;
          serverDone = true;
          clientDone = true;
        } else if (status == null) {
          clientDone = true;
        }
        return false;
      }
      default:
        argument = client.restOfLine();
        break;
    }
    session.commands.add(new String[] {tag, command, argument}, context);
    return sasl;
  }

  /** Reads server responses until the completion of stopTag (whose status is returned) or the end. */
  private String readServer(String stopTag) {
    while (!serverDone && !server.atEnd()) {
      int start = server.position();
      try {
        String status = response(start);
        if (status != null && stopTag != null && stopTag.equals(lastTag)) {
          return status;
        }
      } catch (ImapReader.Problem p) {
        problem(p, start, "server", "response");
        serverDone = true;
      }
    }
    return null;
  }

  /** Reads one response; returns the status of a tagged completion, otherwise null. */
  private String response(int start) {
    String first = server.token();
    if (first == null) {
      throw new ImapReader.Problem(ImapReader.Problem.Kind.SYNTAX, -1);
    }
    if (first.equals("*")) {
      String second = server.token();
      if (second == null) {
        throw new ImapReader.Problem(ImapReader.Problem.Kind.SYNTAX, -1);
      }
      Long number = MailFields.number(second);
      if (number != null) {
        String third = server.token();
        if (third != null && third.equalsIgnoreCase("FETCH")) {
          fetch(number, server.values());
        } else {
          server.restOfLine();
        }
        return null;
      }
      String kind = second.toUpperCase(Locale.ROOT);
      String text = server.restOfLine();
      if (kind.equals("CAPABILITY")) {
        capabilities(text);
      } else if (kind.equals("OK") || kind.equals("PREAUTH") || kind.equals("BYE")) {
        if (start == 0 && !text.isEmpty()) {
          session.banner = text;
        }
        capabilityCode(text);
      }
      return null;
    }
    if (first.equals("+")) {
      server.restOfLine();
      return null;
    }
    String status = server.token();
    if (!TAG.matcher(first).matches() || status == null) {
      throw new ImapReader.Problem(ImapReader.Problem.Kind.SYNTAX, -1);
    }
    status = status.toUpperCase(Locale.ROOT);
    if (!status.equals("OK") && !status.equals("NO") && !status.equals("BAD")) {
      throw new ImapReader.Problem(ImapReader.Problem.Kind.SYNTAX, -1);
    }
    String text = server.restOfLine();
    session.responses.add(new String[] {first, status, text}, context);
    if (status.equals("OK")) {
      capabilityCode(text);
    }
    for (String[] select : selects) {
      if (select[0].equals(first) && select[2] == null) {
        select[2] = status;
      }
    }
    lastTag = first;
    return status;
  }

  private void capabilityCode(String text) {
    Matcher m = CAPABILITY_CODE.matcher(text);
    if (m.find()) {
      capabilities(m.group(1));
    }
  }

  private void capabilities(String text) {
    session.capabilities.clear();
    for (String capability : text.trim().split(" +")) {
      if (!capability.isEmpty()) {
        session.capabilities.add(capability, context);
      }
    }
  }

  private void problem(ImapReader.Problem p, int start, String direction, String what) {
    switch (p.kind) {
      case LITERAL:
        context.warn("literal of " + p.literalLength + " bytes at byte " + start + " exceeds the " + direction
            + " stream");
        break;
      case SYNTAX:
        context.warn("unparseable " + what + " at byte " + start + " of " + direction + " stream");
        break;
      default:
        break; // incomplete data at the end of the stream
    }
  }

  /** Extracts the envelope and header fields of a FETCH response. */
  @SuppressWarnings("unchecked")
  private void fetch(long number, List<Object> values) {
    if (values.size() != 1 || !(values.get(0) instanceof List)) {
      return;
    }
    List<Object> items = (List<Object>) values.get(0);
    MailMessage message = new MailMessage();
    message.number = number <= Integer.MAX_VALUE ? (int) number : null;
    boolean found = false;
    for (int i = 0; i + 1 < items.size(); i += 2) {
      if (!(items.get(i) instanceof ImapReader.Atom)) {
        break;
      }
      String name = ((ImapReader.Atom) items.get(i)).text.toUpperCase(Locale.ROOT);
      Object value = items.get(i + 1);
      if (name.equals("UID")) {
        message.uid = MailFields.number(ImapReader.string(value));
      } else if (name.equals("RFC822.SIZE")) {
        message.size = MailFields.number(ImapReader.string(value));
      } else if (name.equals("ENVELOPE") && value instanceof List) {
        envelope((List<Object>) value, message.headers);
        found = true;
      } else if (name.startsWith("BODY[HEADER") || name.startsWith("BODY[]") || name.equals("RFC822")
          || name.equals("RFC822.HEADER")) {
        MailHeaders headers = headers(value);
        if (headers != null) {
          merge(message.headers, headers);
          found = true;
        }
      }
    }
    if (found) {
      session.fetched.add(message, context);
    }
  }

  private static MailHeaders headers(Object value) {
    if (value instanceof ImapReader.Literal) {
      ImapReader.Literal l = (ImapReader.Literal) value;
      return MailHeaders.parse(l.data, l.start, l.start + l.length);
    }
    if (value instanceof String) {
      byte[] bytes = ((String) value).getBytes(StandardCharsets.UTF_8);
      return MailHeaders.parse(bytes, 0, bytes.length);
    }
    return null;
  }

  /** Fills fields the envelope did not give from the message headers. */
  private static void merge(MailHeaders target, MailHeaders source) {
    target.from = target.from != null ? target.from : source.from;
    target.to = target.to != null ? target.to : source.to;
    target.cc = target.cc != null ? target.cc : source.cc;
    target.subject = target.subject != null ? target.subject : source.subject;
    target.date = target.date != null ? target.date : source.date;
    target.messageId = target.messageId != null ? target.messageId : source.messageId;
  }

  /** ENVELOPE: (date subject from sender reply-to to cc bcc in-reply-to message-id) */
  private static void envelope(List<Object> e, MailHeaders h) {
    h.date = e.size() > 0 ? ImapReader.string(e.get(0)) : null;
    h.subject = e.size() > 1 ? MailLines.cap(MailHeaders.decodeWords(ImapReader.string(e.get(1)))) : null;
    h.from = e.size() > 2 ? addresses(e.get(2)) : null;
    h.to = e.size() > 5 ? addresses(e.get(5)) : null;
    h.cc = e.size() > 6 ? addresses(e.get(6)) : null;
    h.messageId = e.size() > 9 ? ImapReader.string(e.get(9)) : null;
  }

  /** An address list ((name adl mailbox host) ...) as "Name <mailbox@host>, ...". */
  @SuppressWarnings("unchecked")
  private static String addresses(Object value) {
    if (!(value instanceof List)) {
      return null;
    }
    StringBuilder out = new StringBuilder();
    for (Object a : (List<Object>) value) {
      if (!(a instanceof List) || ((List<Object>) a).size() != 4) {
        continue;
      }
      List<Object> address = (List<Object>) a;
      String name = ImapReader.string(address.get(0));
      String mailbox = ImapReader.string(address.get(2));
      String host = ImapReader.string(address.get(3));
      if (mailbox == null || host == null) {
        continue; // start or end of a group
      }
      if (out.length() > 0) {
        out.append(", ");
      }
      if (name != null && !name.isEmpty()) {
        out.append(MailHeaders.decodeWords(name)).append(" <").append(mailbox).append('@').append(host).append('>');
      } else {
        out.append(mailbox).append('@').append(host);
      }
      if (out.length() > MailLines.MAX_STRING) {
        break;
      }
    }
    return out.length() == 0 ? null : MailLines.cap(out.toString());
  }
}
