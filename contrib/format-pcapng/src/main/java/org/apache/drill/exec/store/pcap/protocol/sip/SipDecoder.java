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
package org.apache.drill.exec.store.pcap.protocol.sip;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.PacketProtocolDecoder;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/** SIP over UDP and TCP port 5060. Port 5061 carries SIP over TLS and is not decoded. */
public class SipDecoder implements PacketProtocolDecoder<SipMessage> {
  private static final int PORT = 5060;

  @Override
  public String protocol() {
    return "sip";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("is_request", MinorType.BIT)
        .addNullable("method", MinorType.VARCHAR)
        .addNullable("request_uri", MinorType.VARCHAR)
        .addNullable("status_code", MinorType.INT)
        .addNullable("reason", MinorType.VARCHAR)
        .addNullable("from_address", MinorType.VARCHAR)
        .addNullable("to_address", MinorType.VARCHAR)
        .addNullable("call_id", MinorType.VARCHAR)
        .addNullable("cseq", MinorType.VARCHAR)
        .addNullable("user_agent", MinorType.VARCHAR)
        .addNullable("contact", MinorType.VARCHAR)
        .addArray("via", MinorType.VARCHAR)
        .addNullable("content_type", MinorType.VARCHAR)
        .addNullable("content_length", MinorType.BIGINT)
        .addNullable("username", MinorType.VARCHAR)
        .addNullable("password_present", MinorType.BIT)
        .addMapArray("headers")
          .addNullable("name", MinorType.VARCHAR)
          .addNullable("value", MinorType.VARCHAR)
          .resumeSchema();
  }

  @Override
  public boolean accepts(Packet packet) {
    return (packet.isUdpPacket() || packet.isTcpPacket())
        && (packet.getSrc_port() == PORT || packet.getDst_port() == PORT);
  }

  @Override
  public SipMessage parse(Packet packet, byte[] payload, DecoderContext context) {
    return SipParser.parse(payload, context);
  }

  @Override
  public void write(SipMessage m, TupleWriter fields) {
    fields.scalar("is_request").setBoolean(m.isRequest);
    setString(fields, "method", m.method);
    setString(fields, "request_uri", m.requestUri);
    if (m.statusCode != null) {
      fields.scalar("status_code").setInt(m.statusCode);
    }
    setString(fields, "reason", m.reason);
    setString(fields, "from_address", m.from);
    setString(fields, "to_address", m.to);
    setString(fields, "call_id", m.callId);
    setString(fields, "cseq", m.cseq);
    setString(fields, "user_agent", m.userAgent);
    setString(fields, "contact", m.contact);
    ScalarWriter via = fields.array("via").scalar();
    for (String v : m.via) {
      via.setString(v);
    }
    setString(fields, "content_type", m.contentType);
    if (m.contentLength != null) {
      fields.scalar("content_length").setLong(m.contentLength);
    }
    if (m.username != null) {
      fields.scalar("username").setString(m.username);
      fields.scalar("password_present").setBoolean(m.passwordPresent);
    }
    ArrayWriter headers = fields.array("headers");
    for (String[] h : m.headers) {
      TupleWriter t = headers.tuple();
      t.scalar("name").setString(h[0]);
      t.scalar("value").setString(h[1]);
      headers.save();
    }
  }

  private static void setString(TupleWriter fields, String name, String value) {
    if (value != null) {
      fields.scalar(name).setString(value);
    }
  }
}
