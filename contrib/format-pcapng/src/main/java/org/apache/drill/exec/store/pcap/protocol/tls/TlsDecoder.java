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
package org.apache.drill.exec.store.pcap.protocol.tls;

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
 * The TLS ClientHello and ServerHello at the start of a TCP segment, with JA3 and JA3S fingerprints.
 * Ports: HTTPS (443, 8443), SMTPS submission (465), NNTPS (563), LDAPS (636), DNS over TLS (853),
 * FTPS (989, 990), Telnet over TLS (992), IMAPS (993), IRCS (994), POP3S (995) and SIP over TLS (5061).
 */
public class TlsDecoder implements PacketProtocolDecoder<TlsHello> {
  private static final Set<Integer> PORTS =
      new HashSet<>(Arrays.asList(443, 465, 563, 636, 853, 989, 990, 992, 993, 994, 995, 5061, 8443));

  @Override
  public String protocol() {
    return "tls";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("handshake_type", MinorType.VARCHAR)
        .addNullable("record_version", MinorType.VARCHAR)
        .addNullable("version", MinorType.VARCHAR)
        .addArray("supported_versions", MinorType.VARCHAR)
        .addNullable("session_id", MinorType.VARCHAR)
        .addNullable("sni", MinorType.VARCHAR)
        .addArray("alpn", MinorType.VARCHAR)
        .addArray("cipher_suites", MinorType.INT)
        .addNullable("cipher_suite", MinorType.INT)
        .addArray("extensions", MinorType.INT)
        .addArray("supported_groups", MinorType.INT)
        .addArray("ec_point_formats", MinorType.INT)
        .addArray("signature_algorithms", MinorType.INT)
        .addNullable("ja3", MinorType.VARCHAR)
        .addNullable("ja3_hash", MinorType.VARCHAR)
        .addNullable("ja3s", MinorType.VARCHAR)
        .addNullable("ja3s_hash", MinorType.VARCHAR)
        .addNullable("ja4", MinorType.VARCHAR)
        .addNullable("ja4_raw", MinorType.VARCHAR);
  }

  @Override
  public boolean accepts(Packet packet) {
    return packet.isTcpPacket() && (PORTS.contains(packet.getSrc_port()) || PORTS.contains(packet.getDst_port()));
  }

  @Override
  public TlsHello parse(Packet packet, byte[] payload, DecoderContext context) {
    return TlsParser.parse(payload, context);
  }

  @Override
  public void write(TlsHello h, TupleWriter fields) {
    setString(fields, "handshake_type", h.handshakeType);
    setString(fields, "record_version", h.recordVersion);
    setString(fields, "version", h.version);
    setStrings(fields, "supported_versions", h.supportedVersions);
    setString(fields, "session_id", h.sessionId);
    setString(fields, "sni", h.sni);
    setStrings(fields, "alpn", h.alpn);
    setInts(fields, "cipher_suites", h.cipherSuites);
    if (h.cipherSuite != null) {
      fields.scalar("cipher_suite").setInt(h.cipherSuite);
    }
    setInts(fields, "extensions", h.extensions);
    setInts(fields, "supported_groups", h.supportedGroups);
    setInts(fields, "ec_point_formats", h.ecPointFormats);
    setInts(fields, "signature_algorithms", h.signatureAlgorithms);
    setString(fields, "ja3", h.ja3);
    setString(fields, "ja3_hash", h.ja3Hash);
    setString(fields, "ja3s", h.ja3s);
    setString(fields, "ja3s_hash", h.ja3sHash);
    setString(fields, "ja4", h.ja4);
    setString(fields, "ja4_raw", h.ja4Raw);
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
