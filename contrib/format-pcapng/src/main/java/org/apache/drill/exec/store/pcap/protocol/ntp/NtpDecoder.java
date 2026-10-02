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
package org.apache.drill.exec.store.pcap.protocol.ntp;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.PacketProtocolDecoder;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/** NTP over UDP port 123. */
public class NtpDecoder implements PacketProtocolDecoder<NtpMessage> {
  private static final int PORT = 123;

  @Override
  public String protocol() {
    return "ntp";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("leap_indicator", MinorType.INT)
        .addNullable("version", MinorType.INT)
        .addNullable("mode", MinorType.VARCHAR)
        .addNullable("stratum", MinorType.INT)
        .addNullable("poll", MinorType.INT)
        .addNullable("precision", MinorType.INT)
        .addNullable("root_delay", MinorType.FLOAT8)
        .addNullable("root_dispersion", MinorType.FLOAT8)
        .addNullable("reference_id", MinorType.VARCHAR)
        .addNullable("reference_time", MinorType.TIMESTAMP)
        .addNullable("origin_time", MinorType.TIMESTAMP)
        .addNullable("receive_time", MinorType.TIMESTAMP)
        .addNullable("transmit_time", MinorType.TIMESTAMP)
        .addNullable("request_code", MinorType.INT);
  }

  @Override
  public boolean accepts(Packet packet) {
    return packet.isUdpPacket() && (packet.getSrc_port() == PORT || packet.getDst_port() == PORT);
  }

  @Override
  public NtpMessage parse(Packet packet, byte[] payload, DecoderContext context) {
    return NtpParser.parse(payload, context);
  }

  @Override
  public void write(NtpMessage m, TupleWriter fields) {
    if (m.leapIndicator != null) {
      fields.scalar("leap_indicator").setInt(m.leapIndicator);
    }
    fields.scalar("version").setInt(m.version);
    fields.scalar("mode").setString(m.mode);
    if (m.stratum != null) {
      fields.scalar("stratum").setInt(m.stratum);
      fields.scalar("poll").setInt(m.poll);
      fields.scalar("precision").setInt(m.precision);
      fields.scalar("root_delay").setDouble(m.rootDelay);
      fields.scalar("root_dispersion").setDouble(m.rootDispersion);
    }
    if (m.referenceId != null) {
      fields.scalar("reference_id").setString(m.referenceId);
    }
    if (m.referenceTime != null) {
      fields.scalar("reference_time").setTimestamp(m.referenceTime);
    }
    if (m.originTime != null) {
      fields.scalar("origin_time").setTimestamp(m.originTime);
    }
    if (m.receiveTime != null) {
      fields.scalar("receive_time").setTimestamp(m.receiveTime);
    }
    if (m.transmitTime != null) {
      fields.scalar("transmit_time").setTimestamp(m.transmitTime);
    }
    if (m.requestCode != null) {
      fields.scalar("request_code").setInt(m.requestCode);
    }
  }
}
