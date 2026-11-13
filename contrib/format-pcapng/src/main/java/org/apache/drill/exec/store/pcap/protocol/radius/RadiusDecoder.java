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
package org.apache.drill.exec.store.pcap.protocol.radius;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.PacketProtocolDecoder;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/** RADIUS authentication and accounting over UDP 1812, 1813 and the legacy 1645, 1646. */
public class RadiusDecoder implements PacketProtocolDecoder<RadiusMessage> {
  private static final Set<Integer> PORTS = new HashSet<>(Arrays.asList(1812, 1813, 1645, 1646));

  @Override
  public String protocol() {
    return "radius";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("code", MinorType.INT)
        .addNullable("code_name", MinorType.VARCHAR)
        .addNullable("identifier", MinorType.INT)
        .addNullable("authenticator", MinorType.VARCHAR)
        .addNullable("username", MinorType.VARCHAR)
        .addNullable("password_present", MinorType.BIT)
        .addNullable("nas_ip_address", MinorType.VARCHAR)
        .addNullable("nas_identifier", MinorType.VARCHAR)
        .addNullable("nas_port", MinorType.BIGINT)
        .addNullable("calling_station_id", MinorType.VARCHAR)
        .addNullable("called_station_id", MinorType.VARCHAR)
        .addNullable("framed_ip_address", MinorType.VARCHAR)
        .addNullable("acct_status_type", MinorType.VARCHAR)
        .addNullable("acct_session_id", MinorType.VARCHAR)
        .addNullable("reply_message", MinorType.VARCHAR)
        .addMapArray("attributes")
          .addNullable("type", MinorType.INT)
          .addNullable("value", MinorType.VARCHAR)
          .resumeSchema();
  }

  @Override
  public boolean accepts(Packet packet) {
    return packet.isUdpPacket() && (PORTS.contains(packet.getSrc_port()) || PORTS.contains(packet.getDst_port()));
  }

  @Override
  public RadiusMessage parse(Packet packet, byte[] payload, DecoderContext context) {
    return payload == null ? null : RadiusParser.parse(payload, context);
  }

  @Override
  public void write(RadiusMessage m, TupleWriter fields) {
    fields.scalar("code").setInt(m.code);
    fields.scalar("code_name").setString(m.codeName);
    fields.scalar("identifier").setInt(m.identifier);
    fields.scalar("authenticator").setString(m.authenticator);
    setString(fields, "username", m.username);
    fields.scalar("password_present").setBoolean(m.passwordPresent);
    setString(fields, "nas_ip_address", m.nasIpAddress);
    setString(fields, "nas_identifier", m.nasIdentifier);
    if (m.nasPort != null) {
      fields.scalar("nas_port").setLong(m.nasPort);
    }
    setString(fields, "calling_station_id", m.callingStationId);
    setString(fields, "called_station_id", m.calledStationId);
    setString(fields, "framed_ip_address", m.framedIpAddress);
    setString(fields, "acct_status_type", m.acctStatusType);
    setString(fields, "acct_session_id", m.acctSessionId);
    setString(fields, "reply_message", m.replyMessage);
    ArrayWriter attributes = fields.array("attributes");
    for (RadiusMessage.Attribute a : m.attributes) {
      TupleWriter t = attributes.tuple();
      t.scalar("type").setInt(a.type);
      setString(t, "value", a.value);
      attributes.save();
    }
  }

  private static void setString(TupleWriter fields, String name, String value) {
    if (value != null) {
      fields.scalar(name).setString(value);
    }
  }
}
