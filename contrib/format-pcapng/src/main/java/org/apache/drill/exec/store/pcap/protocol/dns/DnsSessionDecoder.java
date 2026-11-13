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
package org.apache.drill.exec.store.pcap.protocol.dns;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.TcpSession;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.SessionProtocolDecoder;
import org.apache.drill.exec.store.pcap.protocol.TcpStream;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * DNS over TCP (RFC 1035 section 4.2.2, port 53): every length-prefixed message of a session, including the
 * many responses of a zone transfer.
 */
public class DnsSessionDecoder implements SessionProtocolDecoder<DnsSession> {
  static final int PORT = 53;
  static final int MAX_MESSAGES = 1000;
  private static final int HEADER = 12;
  private static final Set<String> ZONE_TRANSFERS = new HashSet<>(Arrays.asList("AXFR", "IXFR", "251"));

  @Override
  public String protocol() {
    return "dns";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addMapArray("queries")
          .addNullable("transaction_id", MinorType.INT)
          .addNullable("name", MinorType.VARCHAR)
          .addNullable("type", MinorType.VARCHAR)
          .resumeSchema()
        .addMapArray("answers")
          .addNullable("transaction_id", MinorType.INT)
          .addNullable("name", MinorType.VARCHAR)
          .addNullable("type", MinorType.VARCHAR)
          .addNullable("ttl", MinorType.BIGINT)
          .addNullable("data", MinorType.VARCHAR)
          .resumeSchema()
        .addNullable("client_message_count", MinorType.INT)
        .addNullable("server_message_count", MinorType.INT)
        .addNullable("is_zone_transfer", MinorType.BIT);
  }

  @Override
  public boolean accepts(TcpSession session) {
    return session.getSrcPort() == PORT || session.getDstPort() == PORT;
  }

  @Override
  public DnsSession parse(TcpStream fromClient, TcpStream fromServer, DecoderContext context) {
    return parseStreams(fromClient.data(), fromClient.firstGap(), fromServer.data(), fromServer.firstGap(), context);
  }

  /**
   * @param clientGap offset of the first missing byte in the client stream, or -1
   * @return null if neither stream starts with a DNS message
   * @throws IllegalArgumentException if the session is DNS but no message could be parsed
   */
  static DnsSession parseStreams(byte[] client, long clientGap, byte[] server, long serverGap,
                                 DecoderContext context) {
    DecoderContext once = new OnceContext(context);
    DnsSession s = new DnsSession();
    String clientError = readStream(client, clientGap, true, s, once);
    String serverError = readStream(server, serverGap, false, s, once);
    if (s.clientMessages + s.serverMessages == 0) {
      if (clientError != null || serverError != null) {
        throw new IllegalArgumentException(clientError != null ? clientError : serverError);
      }
      return null;
    }
    for (String error : new String[] {clientError, serverError}) {
      if (error != null) {
        once.warn(error);
      }
    }
    return s;
  }

  /**
   * Reads consecutive messages until the data ends or stops making sense.
   *
   * @return a problem with the message after the last good one that shows the stream is DNS, or null
   */
  private static String readStream(byte[] data, long gap, boolean fromClient, DnsSession s, DecoderContext context) {
    String direction = fromClient ? "client" : "server";
    int count = 0;
    int offset = 0;
    while (offset < data.length) {
      if (count == MAX_MESSAGES) {
        context.warn("stopped after " + MAX_MESSAGES + " messages in " + direction + " stream");
        break;
      }
      int length = data.length - offset < 2 ? -1 : ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
      if (length >= 0 && length < HEADER) {
        if (count > 0) {
          context.warn("unparseable data in " + direction + " stream at byte " + offset);
        }
        break;
      }
      if (length < 0 || length > data.length - offset - 2) {
        // The message runs past the captured data
        String problem = gap >= 0 ? "stopped at missing data in " + direction + " stream at byte " + gap
            : "truncated message in " + direction + " stream at byte " + offset;
        if (count > 0) {
          context.warn(problem);
          break;
        }
        // Report it only if the part we have looks like DNS
        try {
          if (length >= 0 && DnsParser.parse(Arrays.copyOfRange(data, offset + 2, data.length), 0, context) == null) {
            return null;
          }
        } catch (IllegalArgumentException e) {
          return problem;
        }
        return length < 0 ? null : problem;
      }
      DnsMessage m;
      try {
        m = DnsParser.parse(Arrays.copyOfRange(data, offset + 2, offset + 2 + length), 0, context);
      } catch (IllegalArgumentException e) {
        return direction + " message " + (count + 1) + ": " + e.getMessage();
      }
      if (m == null) {
        if (count > 0) {
          context.warn("unparseable data in " + direction + " stream at byte " + offset);
        }
        break;
      }
      count++;
      add(s, m, fromClient, context);
      offset += 2 + length;
    }
    return null;
  }

  private static void add(DnsSession s, DnsMessage m, boolean fromClient, DecoderContext context) {
    for (DnsMessage.Question q : m.questions) {
      if (ZONE_TRANSFERS.contains(q.type)) {
        s.isZoneTransfer = true;
      }
    }
    if (fromClient) {
      s.clientMessages++;
      for (DnsMessage.Question q : m.questions) {
        if (s.queries.size() == DnsParser.MAX_ITEMS) {
          context.warn("queries truncated to " + DnsParser.MAX_ITEMS);
          break;
        }
        DnsSession.Query query = new DnsSession.Query();
        query.transactionId = m.transactionId;
        query.name = q.name;
        query.type = q.type;
        s.queries.add(query);
      }
    } else {
      s.serverMessages++;
      for (DnsMessage.Record r : m.answers) {
        if (s.answers.size() == DnsParser.MAX_ITEMS) {
          context.warn("answers truncated to " + DnsParser.MAX_ITEMS);
          break;
        }
        DnsSession.Answer a = new DnsSession.Answer();
        a.transactionId = m.transactionId;
        a.name = r.name;
        a.type = r.type;
        a.ttl = r.ttl;
        a.data = r.data;
        s.answers.add(a);
      }
    }
  }

  @Override
  public void write(DnsSession s, TupleWriter fields) {
    ArrayWriter queries = fields.array("queries");
    for (DnsSession.Query q : s.queries) {
      TupleWriter t = queries.tuple();
      t.scalar("transaction_id").setInt(q.transactionId);
      t.scalar("name").setString(q.name);
      t.scalar("type").setString(q.type);
      queries.save();
    }
    ArrayWriter answers = fields.array("answers");
    for (DnsSession.Answer a : s.answers) {
      TupleWriter t = answers.tuple();
      t.scalar("transaction_id").setInt(a.transactionId);
      t.scalar("name").setString(a.name);
      t.scalar("type").setString(a.type);
      t.scalar("ttl").setLong(a.ttl);
      t.scalar("data").setString(a.data);
      answers.save();
    }
    fields.scalar("client_message_count").setInt(s.clientMessages);
    fields.scalar("server_message_count").setInt(s.serverMessages);
    fields.scalar("is_zone_transfer").setBoolean(s.isZoneTransfer);
  }

  /** Passes each distinct warning on once: every message of a zone transfer may hit the same cap. */
  private static final class OnceContext implements DecoderContext {
    private final DecoderContext context;
    private final Set<String> seen = new HashSet<>();

    OnceContext(DecoderContext context) {
      this.context = context;
    }

    @Override
    public boolean exposeCredentials() {
      return context.exposeCredentials();
    }

    @Override
    public void warn(String message) {
      if (seen.add(message)) {
        context.warn(message);
      }
    }
  }
}
