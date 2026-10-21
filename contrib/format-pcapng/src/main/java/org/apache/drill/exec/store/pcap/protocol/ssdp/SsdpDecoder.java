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
package org.apache.drill.exec.store.pcap.protocol.ssdp;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.PacketProtocolDecoder;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/** SSDP (UPnP discovery) over UDP port 1900. */
public class SsdpDecoder implements PacketProtocolDecoder<SsdpMessage> {
  private static final int PORT = 1900;

  @Override
  public String protocol() {
    return "ssdp";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("method", MinorType.VARCHAR)
        .addNullable("status_code", MinorType.INT)
        .addNullable("st", MinorType.VARCHAR)
        .addNullable("nt", MinorType.VARCHAR)
        .addNullable("nts", MinorType.VARCHAR)
        .addNullable("usn", MinorType.VARCHAR)
        .addNullable("location", MinorType.VARCHAR)
        .addNullable("server", MinorType.VARCHAR)
        .addNullable("user_agent", MinorType.VARCHAR)
        .addNullable("man", MinorType.VARCHAR)
        .addNullable("mx", MinorType.INT)
        .addNullable("cache_control", MinorType.VARCHAR)
        .addMapArray("headers")
          .addNullable("name", MinorType.VARCHAR)
          .addNullable("value", MinorType.VARCHAR)
          .resumeSchema();
  }

  @Override
  public boolean accepts(Packet packet) {
    return packet.isUdpPacket() && (packet.getSrc_port() == PORT || packet.getDst_port() == PORT);
  }

  @Override
  public SsdpMessage parse(Packet packet, byte[] payload, DecoderContext context) {
    return SsdpParser.parse(payload, context);
  }

  @Override
  public void write(SsdpMessage m, TupleWriter fields) {
    setString(fields, "method", m.method);
    if (m.statusCode != null) {
      fields.scalar("status_code").setInt(m.statusCode);
    }
    setString(fields, "st", m.st);
    setString(fields, "nt", m.nt);
    setString(fields, "nts", m.nts);
    setString(fields, "usn", m.usn);
    setString(fields, "location", m.location);
    setString(fields, "server", m.server);
    setString(fields, "user_agent", m.userAgent);
    setString(fields, "man", m.man);
    if (m.mx != null) {
      fields.scalar("mx").setInt(m.mx);
    }
    setString(fields, "cache_control", m.cacheControl);
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
