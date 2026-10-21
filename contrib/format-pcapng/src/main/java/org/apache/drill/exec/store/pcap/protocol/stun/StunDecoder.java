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
package org.apache.drill.exec.store.pcap.protocol.stun;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.PacketProtocolDecoder;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * STUN (RFC 5389 and 8489, including the TURN methods of RFC 8656) over UDP ports 3478 (STUN and TURN)
 * and 19302 (Google's public STUN servers). Classic RFC 3489 STUN, which has no magic cookie, is not decoded.
 */
public class StunDecoder implements PacketProtocolDecoder<StunMessage> {
  private static final int STUN_PORT = 3478;
  private static final int GOOGLE_STUN_PORT = 19302;

  @Override
  public String protocol() {
    return "stun";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("message_class", MinorType.VARCHAR)
        .addNullable("message_method", MinorType.VARCHAR)
        .addNullable("transaction_id", MinorType.VARCHAR)
        .addNullable("xor_mapped_address", MinorType.VARCHAR)
        .addNullable("mapped_address", MinorType.VARCHAR)
        .addNullable("software", MinorType.VARCHAR)
        .addNullable("realm", MinorType.VARCHAR)
        .addNullable("nonce", MinorType.VARCHAR)
        .addNullable("error_code", MinorType.INT)
        .addNullable("error_reason", MinorType.VARCHAR)
        .addNullable("username", MinorType.VARCHAR)
        .addNullable("password_present", MinorType.BIT)
        .addMapArray("attributes")
          .addNullable("type", MinorType.INT)
          .addNullable("length", MinorType.INT)
          .resumeSchema();
  }

  @Override
  public boolean accepts(Packet packet) {
    return packet.isUdpPacket() && (isStunPort(packet.getSrc_port()) || isStunPort(packet.getDst_port()));
  }

  private static boolean isStunPort(int port) {
    return port == STUN_PORT || port == GOOGLE_STUN_PORT;
  }

  @Override
  public StunMessage parse(Packet packet, byte[] payload, DecoderContext context) {
    return StunParser.parse(payload, context);
  }

  @Override
  public void write(StunMessage m, TupleWriter fields) {
    setString(fields, "message_class", m.messageClass);
    setString(fields, "message_method", m.messageMethod);
    setString(fields, "transaction_id", m.transactionId);
    setString(fields, "xor_mapped_address", m.xorMappedAddress);
    setString(fields, "mapped_address", m.mappedAddress);
    setString(fields, "software", m.software);
    setString(fields, "realm", m.realm);
    setString(fields, "nonce", m.nonce);
    if (m.errorCode != null) {
      fields.scalar("error_code").setInt(m.errorCode);
    }
    setString(fields, "error_reason", m.errorReason);
    setString(fields, "username", m.username);
    // STUN authenticates with an HMAC; no cleartext password is ever sent
    fields.scalar("password_present").setBoolean(false);
    ArrayWriter attributes = fields.array("attributes");
    for (int[] a : m.attributes) {
      TupleWriter t = attributes.tuple();
      t.scalar("type").setInt(a[0]);
      t.scalar("length").setInt(a[1]);
      attributes.save();
    }
  }

  private static void setString(TupleWriter fields, String name, String value) {
    if (value != null) {
      fields.scalar(name).setString(value);
    }
  }
}
