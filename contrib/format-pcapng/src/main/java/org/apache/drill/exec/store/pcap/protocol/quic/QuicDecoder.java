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
package org.apache.drill.exec.store.pcap.protocol.quic;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.PacketProtocolDecoder;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * QUIC long-header packets over UDP, including the version 1 Initial, whose TLS ClientHello is decrypted with
 * the Initial keys (RFC 9001 section 5.2). Those keys are derived from values that travel in the clear, so this
 * is passive inspection: no negotiated user traffic is ever decrypted. Accepts the common HTTP/3 ports (80, 443,
 * 8443) and any UDP datagram whose first byte is a QUIC long header with the Initial type.
 */
public class QuicDecoder implements PacketProtocolDecoder<QuicInitial> {
  private static final Set<Integer> PORTS = new HashSet<>(Arrays.asList(80, 443, 8443));

  @Override
  public String protocol() {
    return "quic";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("packet_type", MinorType.VARCHAR)
        .addNullable("version", MinorType.VARCHAR)
        .addNullable("dcid", MinorType.VARCHAR)
        .addNullable("scid", MinorType.VARCHAR)
        .addNullable("sni", MinorType.VARCHAR)
        .addArray("alpn", MinorType.VARCHAR)
        .addArray("supported_versions", MinorType.VARCHAR)
        .addArray("cipher_suites", MinorType.INT)
        .addNullable("ja4", MinorType.VARCHAR);
  }

  @Override
  public boolean accepts(Packet packet) {
    if (!packet.isUdpPacket()) {
      return false;
    }
    if (PORTS.contains(packet.getSrc_port()) || PORTS.contains(packet.getDst_port())) {
      return true;
    }
    byte[] data = packet.getData();
    // A QUIC version 1 long header with the Initial type: header form and fixed bit set, type bits zero
    return data != null && data.length > 0 && (data[0] & 0xF0) == 0xC0;
  }

  @Override
  public QuicInitial parse(Packet packet, byte[] payload, DecoderContext context) {
    return QuicParser.parse(payload, context);
  }

  @Override
  public void write(QuicInitial q, TupleWriter fields) {
    setString(fields, "packet_type", q.packetType);
    setString(fields, "version", q.version);
    setString(fields, "dcid", q.dcid);
    setString(fields, "scid", q.scid);
    setString(fields, "sni", q.sni);
    setStrings(fields, "alpn", q.alpn);
    setStrings(fields, "supported_versions", q.supportedVersions);
    setInts(fields, "cipher_suites", q.cipherSuites);
    setString(fields, "ja4", q.ja4);
  }

  private static void setString(TupleWriter fields, String name, String value) {
    if (value != null) {
      fields.scalar(name).setString(value);
    }
  }

  private static void setStrings(TupleWriter fields, String name, List<String> values) {
    if (values != null) {
      ScalarWriter element = fields.array(name).scalar();
      for (String v : values) {
        element.setString(v);
      }
    }
  }

  private static void setInts(TupleWriter fields, String name, List<Integer> values) {
    if (values != null) {
      ScalarWriter element = fields.array(name).scalar();
      for (int v : values) {
        element.setInt(v);
      }
    }
  }
}
