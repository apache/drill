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
package org.apache.drill.exec.store.pcap.protocol.snmp;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.PacketProtocolDecoder;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/** SNMP v1, v2c and v3 over UDP 161 (agent) and 162 (traps). */
public class SnmpDecoder implements PacketProtocolDecoder<SnmpMessage> {

  @Override
  public String protocol() {
    return "snmp";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("version", MinorType.VARCHAR)
        .addNullable("community_present", MinorType.BIT)
        .addNullable("community", MinorType.VARCHAR)
        .addNullable("pdu_type", MinorType.VARCHAR)
        .addNullable("request_id", MinorType.BIGINT)
        .addNullable("error_status", MinorType.INT)
        .addNullable("error_index", MinorType.INT)
        .addNullable("non_repeaters", MinorType.INT)
        .addNullable("max_repetitions", MinorType.INT)
        .addNullable("enterprise", MinorType.VARCHAR)
        .addNullable("agent_address", MinorType.VARCHAR)
        .addNullable("generic_trap", MinorType.INT)
        .addNullable("specific_trap", MinorType.BIGINT)
        .addNullable("time_stamp", MinorType.BIGINT)
        .addNullable("msg_id", MinorType.BIGINT)
        .addNullable("msg_user_name", MinorType.VARCHAR)
        .addNullable("security_level", MinorType.VARCHAR)
        .addNullable("engine_id", MinorType.VARCHAR)
        .addNullable("encrypted", MinorType.BIT)
        .addMapArray("varbinds")
          .addNullable("oid", MinorType.VARCHAR)
          .addNullable("value_type", MinorType.VARCHAR)
          .addNullable("value", MinorType.VARCHAR)
          .resumeSchema();
  }

  @Override
  public boolean accepts(Packet packet) {
    if (!packet.isUdpPacket()) {
      return false;
    }
    int src = packet.getSrc_port();
    int dst = packet.getDst_port();
    return src == 161 || dst == 161 || src == 162 || dst == 162;
  }

  @Override
  public SnmpMessage parse(Packet packet, byte[] payload, DecoderContext context) {
    return payload == null ? null : SnmpParser.parse(payload, context);
  }

  @Override
  public void write(SnmpMessage m, TupleWriter fields) {
    setString(fields, "version", m.version);
    fields.scalar("community_present").setBoolean(m.communityPresent);
    setString(fields, "community", m.community);
    setString(fields, "pdu_type", m.pduType);
    setLong(fields, "request_id", m.requestId);
    setInt(fields, "error_status", m.errorStatus);
    setInt(fields, "error_index", m.errorIndex);
    setInt(fields, "non_repeaters", m.nonRepeaters);
    setInt(fields, "max_repetitions", m.maxRepetitions);
    setString(fields, "enterprise", m.enterprise);
    setString(fields, "agent_address", m.agentAddress);
    setInt(fields, "generic_trap", m.genericTrap);
    setLong(fields, "specific_trap", m.specificTrap);
    setLong(fields, "time_stamp", m.timeStamp);
    setLong(fields, "msg_id", m.msgId);
    setString(fields, "msg_user_name", m.msgUserName);
    setString(fields, "security_level", m.securityLevel);
    setString(fields, "engine_id", m.engineId);
    if ("v3".equals(m.version)) {
      fields.scalar("encrypted").setBoolean(m.encrypted);
    }
    ArrayWriter varbinds = fields.array("varbinds");
    for (SnmpMessage.Varbind v : m.varbinds) {
      TupleWriter t = varbinds.tuple();
      setString(t, "oid", v.oid);
      setString(t, "value_type", v.valueType);
      setString(t, "value", v.value);
      varbinds.save();
    }
  }

  private static void setString(TupleWriter fields, String name, String value) {
    if (value != null) {
      fields.scalar(name).setString(value);
    }
  }

  private static void setInt(TupleWriter fields, String name, Integer value) {
    if (value != null) {
      fields.scalar(name).setInt(value);
    }
  }

  private static void setLong(TupleWriter fields, String name, Long value) {
    if (value != null) {
      fields.scalar(name).setLong(value);
    }
  }
}
