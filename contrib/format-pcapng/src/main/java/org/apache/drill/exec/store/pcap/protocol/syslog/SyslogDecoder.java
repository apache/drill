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
package org.apache.drill.exec.store.pcap.protocol.syslog;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.PacketProtocolDecoder;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/** Syslog over UDP port 514. */
public class SyslogDecoder implements PacketProtocolDecoder<SyslogMessage> {
  private static final int PORT = 514;

  @Override
  public String protocol() {
    return "syslog";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("facility", MinorType.INT)
        .addNullable("facility_name", MinorType.VARCHAR)
        .addNullable("severity", MinorType.INT)
        .addNullable("severity_name", MinorType.VARCHAR)
        .addNullable("version", MinorType.INT)
        .addNullable("timestamp_text", MinorType.VARCHAR)
        .addNullable("hostname", MinorType.VARCHAR)
        .addNullable("app_name", MinorType.VARCHAR)
        .addNullable("proc_id", MinorType.VARCHAR)
        .addNullable("msg_id", MinorType.VARCHAR)
        .addNullable("structured_data", MinorType.VARCHAR)
        .addNullable("message", MinorType.VARCHAR);
  }

  @Override
  public boolean accepts(Packet packet) {
    return packet.isUdpPacket() && (packet.getSrc_port() == PORT || packet.getDst_port() == PORT);
  }

  @Override
  public SyslogMessage parse(Packet packet, byte[] payload, DecoderContext context) {
    return SyslogParser.parse(payload, context);
  }

  @Override
  public void write(SyslogMessage m, TupleWriter fields) {
    fields.scalar("facility").setInt(m.facility);
    fields.scalar("facility_name").setString(m.facilityName);
    fields.scalar("severity").setInt(m.severity);
    fields.scalar("severity_name").setString(m.severityName);
    if (m.version != null) {
      fields.scalar("version").setInt(m.version);
    }
    setString(fields, "timestamp_text", m.timestampText);
    setString(fields, "hostname", m.hostname);
    setString(fields, "app_name", m.appName);
    setString(fields, "proc_id", m.procId);
    setString(fields, "msg_id", m.msgId);
    setString(fields, "structured_data", m.structuredData);
    setString(fields, "message", m.message);
  }

  private static void setString(TupleWriter fields, String name, String value) {
    if (value != null) {
      fields.scalar(name).setString(value);
    }
  }
}
