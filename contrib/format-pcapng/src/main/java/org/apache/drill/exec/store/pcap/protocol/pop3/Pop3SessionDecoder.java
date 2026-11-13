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
 * POP3 sessions on port 110 (995 is implicit TLS). A session is POP3 only if the server greets with +OK
 * or the client starts with USER, APOP, CAPA, STLS or AUTH.
 */
public class Pop3SessionDecoder implements SessionProtocolDecoder<Pop3Session> {
  static final int PORT = 110;

  @Override
  public String protocol() {
    return "pop3";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("banner", MinorType.VARCHAR)
        .addArray("capabilities", MinorType.VARCHAR);
    MailCredentials.define(fields);
    fields.addNullable("tls_started", MinorType.BIT)
        .addNullable("message_count", MinorType.INT);
    MapBuilder retrieved = fields.addMapArray("retrieved")
        .addNullable("number", MinorType.INT);
    MailMessage.define(retrieved);
    retrieved.resumeSchema()
        .addNullable("command_count", MinorType.INT)
        .addMapArray("commands")
          .addNullable("command", MinorType.VARCHAR)
          .addNullable("argument", MinorType.VARCHAR)
          .resumeSchema()
        .addMapArray("replies")
          .addNullable("status", MinorType.VARCHAR)
          .addNullable("text", MinorType.VARCHAR)
          .resumeSchema();
  }

  @Override
  public boolean accepts(TcpSession session) {
    return session.getSrcPort() == PORT || session.getDstPort() == PORT;
  }

  @Override
  public Pop3Session parse(TcpStream fromClient, TcpStream fromServer, DecoderContext context) {
    Pop3Session session = Pop3Parser.parse(fromClient.data(), fromServer.data(), context);
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
  public void write(Pop3Session s, TupleWriter fields) {
    MailFields.setString(fields, "banner", s.banner);
    MailFields.writeStrings(fields, "capabilities", s.capabilities.items());
    s.credentials.write(fields);
    fields.scalar("tls_started").setBoolean(s.tlsStarted);
    MailFields.setInt(fields, "message_count", s.messageCount);
    ArrayWriter retrieved = fields.array("retrieved");
    for (MailMessage m : s.retrieved.items()) {
      m.write(retrieved.tuple());
      retrieved.save();
    }
    fields.scalar("command_count").setInt(s.commandCount);
    MailFields.writeRows(fields, "commands", new String[] {"command", "argument"}, s.commands.items());
    MailFields.writeRows(fields, "replies", new String[] {"status", "text"}, s.replies.items());
  }
}
