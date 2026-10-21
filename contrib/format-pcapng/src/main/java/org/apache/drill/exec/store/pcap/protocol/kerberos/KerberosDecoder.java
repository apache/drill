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
package org.apache.drill.exec.store.pcap.protocol.kerberos;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.PacketProtocolDecoder;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * Kerberos v5 (RFC 4120) over UDP and single-segment TCP port 88. Reads only cleartext metadata of the
 * AS-REQ, AS-REP, TGS-REQ, TGS-REP and KRB-ERROR messages, which surfaces reconnaissance, AS-REP roasting
 * (no pre-auth) and Kerberoasting (a ticket enc-part of etype 23, RC4-HMAC).
 */
public class KerberosDecoder implements PacketProtocolDecoder<KerberosMessage> {

  @Override
  public String protocol() {
    return "kerberos";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("message_type", MinorType.VARCHAR)
        .addNullable("realm", MinorType.VARCHAR)
        .addNullable("client_name", MinorType.VARCHAR)
        .addNullable("server_name", MinorType.VARCHAR)
        .addArray("encryption_types", MinorType.INT)
        .addNullable("ticket_encryption_type", MinorType.INT)
        .addNullable("error_code", MinorType.INT)
        .addNullable("error_text", MinorType.VARCHAR)
        .addNullable("pre_auth_present", MinorType.BIT)
        .addNullable("till", MinorType.TIMESTAMP);
  }

  @Override
  public boolean accepts(Packet packet) {
    if (!packet.isUdpPacket() && !packet.isTcpPacket()) {
      return false;
    }
    return packet.getSrc_port() == 88 || packet.getDst_port() == 88;
  }

  @Override
  public KerberosMessage parse(Packet packet, byte[] payload, DecoderContext context) {
    return payload == null ? null : KerberosParser.parse(payload, context);
  }

  @Override
  public void write(KerberosMessage m, TupleWriter fields) {
    if (m.messageType != null) {
      fields.scalar("message_type").setString(m.messageType);
    }
    if (m.realm != null) {
      fields.scalar("realm").setString(m.realm);
    }
    if (m.clientName != null) {
      fields.scalar("client_name").setString(m.clientName);
    }
    if (m.serverName != null) {
      fields.scalar("server_name").setString(m.serverName);
    }
    if (!m.encryptionTypes.isEmpty()) {
      ArrayWriter etypes = fields.array("encryption_types");
      for (int etype : m.encryptionTypes) {
        etypes.scalar().setInt(etype);
      }
    }
    if (m.ticketEncryptionType != null) {
      fields.scalar("ticket_encryption_type").setInt(m.ticketEncryptionType);
    }
    if (m.errorCode != null) {
      fields.scalar("error_code").setInt(m.errorCode);
    }
    if (m.errorText != null) {
      fields.scalar("error_text").setString(m.errorText);
    }
    if (m.preAuthPresent != null) {
      fields.scalar("pre_auth_present").setBoolean(m.preAuthPresent);
    }
    if (m.till != null) {
      fields.scalar("till").setTimestamp(m.till);
    }
  }
}
