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
 * IMAP sessions on port 143 (993 is implicit TLS). A session is IMAP only if the server greets with
 * "* OK", "* PREAUTH" or "* BYE", or the client starts with a tagged CAPABILITY, LOGIN, AUTHENTICATE,
 * STARTTLS, NOOP, ID or LOGOUT.
 */
public class ImapSessionDecoder implements SessionProtocolDecoder<ImapSession> {
  static final int PORT = 143;

  @Override
  public String protocol() {
    return "imap";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("banner", MinorType.VARCHAR)
        .addArray("capabilities", MinorType.VARCHAR);
    MailCredentials.define(fields);
    fields.addNullable("tls_started", MinorType.BIT)
        .addArray("selected_mailboxes", MinorType.VARCHAR)
        .addNullable("command_count", MinorType.INT)
        .addMapArray("commands")
          .addNullable("tag", MinorType.VARCHAR)
          .addNullable("command", MinorType.VARCHAR)
          .addNullable("argument", MinorType.VARCHAR)
          .resumeSchema()
        .addMapArray("responses")
          .addNullable("tag", MinorType.VARCHAR)
          .addNullable("status", MinorType.VARCHAR)
          .addNullable("text", MinorType.VARCHAR)
          .resumeSchema();
    MapBuilder fetched = fields.addMapArray("fetched_messages")
        .addNullable("number", MinorType.INT)
        .addNullable("uid", MinorType.BIGINT);
    MailMessage.define(fetched);
    fetched.resumeSchema();
  }

  @Override
  public boolean accepts(TcpSession session) {
    return session.getSrcPort() == PORT || session.getDstPort() == PORT;
  }

  @Override
  public ImapSession parse(TcpStream fromClient, TcpStream fromServer, DecoderContext context) {
    ImapSession session = ImapParser.parse(fromClient.data(), fromServer.data(), context);
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
  public void write(ImapSession s, TupleWriter fields) {
    MailFields.setString(fields, "banner", s.banner);
    MailFields.writeStrings(fields, "capabilities", s.capabilities.items());
    s.credentials.write(fields);
    fields.scalar("tls_started").setBoolean(s.tlsStarted);
    MailFields.writeStrings(fields, "selected_mailboxes", s.selectedMailboxes.items());
    fields.scalar("command_count").setInt(s.commandCount);
    MailFields.writeRows(fields, "commands", new String[] {"tag", "command", "argument"}, s.commands.items());
    MailFields.writeRows(fields, "responses", new String[] {"tag", "status", "text"}, s.responses.items());
    ArrayWriter fetched = fields.array("fetched_messages");
    for (MailMessage m : s.fetched.items()) {
      m.write(fetched.tuple());
      fetched.save();
    }
  }
}
