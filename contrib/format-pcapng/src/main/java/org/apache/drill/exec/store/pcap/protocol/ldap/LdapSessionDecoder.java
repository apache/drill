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
package org.apache.drill.exec.store.pcap.protocol.ldap;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.TcpSession;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.SessionProtocolDecoder;
import org.apache.drill.exec.store.pcap.protocol.TcpStream;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * LDAP (RFC 4511) sessions on TCP port 389. Reassembles LDAPMessages across segments and reads bind
 * credentials, search bases and RFC 4515 filters, and the DNs of returned entries. Credentials appear in
 * {@code password} only when exposeCredentials is set.
 */
public class LdapSessionDecoder implements SessionProtocolDecoder<LdapSession> {

  @Override
  public String protocol() {
    return "ldap";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("version", MinorType.INT)
        .addNullable("bind_dn", MinorType.VARCHAR)
        .addNullable("auth_type", MinorType.VARCHAR)
        .addNullable("password_present", MinorType.BIT)
        .addNullable("password", MinorType.VARCHAR)
        .addNullable("bind_result_code", MinorType.INT)
        .addNullable("bind_result", MinorType.VARCHAR)
        .addMapArray("searches")
          .addNullable("base_dn", MinorType.VARCHAR)
          .addNullable("scope", MinorType.VARCHAR)
          .addNullable("filter", MinorType.VARCHAR)
          .resumeSchema()
        .addArray("entries_returned", MinorType.VARCHAR)
        .addNullable("operation_count", MinorType.INT);
  }

  @Override
  public boolean accepts(TcpSession session) {
    return session.getSrcPort() == 389 || session.getDstPort() == 389;
  }

  @Override
  public LdapSession parse(TcpStream fromClient, TcpStream fromServer, DecoderContext context) {
    return LdapParser.parse(fromClient.data(), fromClient.firstGap(), fromServer.data(), fromServer.firstGap(),
        context);
  }

  @Override
  public void write(LdapSession s, TupleWriter fields) {
    if (s.version != null) {
      fields.scalar("version").setInt(s.version);
    }
    if (s.bindDn != null) {
      fields.scalar("bind_dn").setString(s.bindDn);
    }
    if (s.authType != null) {
      fields.scalar("auth_type").setString(s.authType);
    }
    fields.scalar("password_present").setBoolean(s.passwordPresent);
    if (s.password != null) {
      fields.scalar("password").setString(s.password);
    }
    if (s.bindResultCode != null) {
      fields.scalar("bind_result_code").setInt(s.bindResultCode);
    }
    if (s.bindResult != null) {
      fields.scalar("bind_result").setString(s.bindResult);
    }
    if (!s.searches.isEmpty()) {
      ArrayWriter searches = fields.array("searches");
      for (LdapSession.Search search : s.searches) {
        TupleWriter t = searches.tuple();
        if (search.baseDn != null) {
          t.scalar("base_dn").setString(search.baseDn);
        }
        if (search.scope != null) {
          t.scalar("scope").setString(search.scope);
        }
        if (search.filter != null) {
          t.scalar("filter").setString(search.filter);
        }
        searches.save();
      }
    }
    if (!s.entriesReturned.isEmpty()) {
      ArrayWriter entries = fields.array("entries_returned");
      for (String dn : s.entriesReturned) {
        entries.scalar().setString(dn);
      }
    }
    fields.scalar("operation_count").setInt(s.operationCount);
  }
}
