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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.MapBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.TcpSession;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.SessionProtocolDecoder;
import org.apache.drill.exec.store.pcap.protocol.TcpStream;
import org.apache.drill.exec.store.pcap.protocol.mail.MailCredentials;
import org.apache.drill.exec.store.pcap.protocol.mail.MailFields;
import org.apache.drill.exec.store.pcap.protocol.mail.MailMessage;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * SMTP sessions on ports 25, 587 and 2525 (465 is implicit TLS). A session is SMTP only if the server
 * greets with 220 or the client starts with HELO or EHLO.
 */
public class SmtpSessionDecoder implements SessionProtocolDecoder<SmtpSession> {
  static final Set<Integer> PORTS = new HashSet<>(Arrays.asList(25, 587, 2525));

  @Override
  public String protocol() {
    return "smtp";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("banner", MinorType.VARCHAR)
        .addNullable("helo", MinorType.VARCHAR)
        .addArray("extensions", MinorType.VARCHAR);
    MailCredentials.define(fields);
    fields.addNullable("tls_started", MinorType.BIT)
        .addNullable("mail_from", MinorType.VARCHAR)
        .addArray("rcpt_to", MinorType.VARCHAR)
        .addNullable("message_count", MinorType.INT);
    MapBuilder messages = fields.addMapArray("messages");
    MailMessage.define(messages);
    messages.resumeSchema()
        .addNullable("command_count", MinorType.INT)
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
    return PORTS.contains(session.getSrcPort()) || PORTS.contains(session.getDstPort());
  }

  @Override
  public SmtpSession parse(TcpStream fromClient, TcpStream fromServer, DecoderContext context) {
    SmtpSession session = SmtpParser.parse(fromClient.data(), fromServer.data(), context);
    if (session != null) {
      if (fromClient.hasGaps()) {
        context.warn("stopped at missing data in client stream at byte " + fromClient.firstGap());
      }
      if (fromServer.hasGaps()) {
        context.warn("stopped at missing data in server stream at byte " + fromServer.firstGap());
      }
    }
    return session;
  }

  @Override
  public void write(SmtpSession s, TupleWriter fields) {
    MailFields.setString(fields, "banner", s.banner);
    MailFields.setString(fields, "helo", s.helo);
    MailFields.writeStrings(fields, "extensions", s.extensions.items());
    s.credentials.write(fields);
    fields.scalar("tls_started").setBoolean(s.tlsStarted);
    MailFields.setString(fields, "mail_from", s.mailFrom);
    MailFields.writeStrings(fields, "rcpt_to", s.rcptTo.items());
    fields.scalar("message_count").setInt(s.messageCount);
    ArrayWriter messages = fields.array("messages");
    for (MailMessage m : s.messages.items()) {
      m.write(messages.tuple());
      messages.save();
    }
    fields.scalar("command_count").setInt(s.commandCount);
    MailFields.writeRows(fields, "commands", new String[] {"command", "argument"}, s.commands.items());
    ArrayWriter replies = fields.array("replies");
    for (SmtpSession.Reply r : s.replies.items()) {
      TupleWriter reply = replies.tuple();
      reply.scalar("code").setInt(r.code);
      MailFields.setString(reply, "text", r.text);
      replies.save();
    }
  }
}
