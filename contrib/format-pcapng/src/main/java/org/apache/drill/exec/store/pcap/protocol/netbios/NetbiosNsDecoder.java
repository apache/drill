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
package org.apache.drill.exec.store.pcap.protocol.netbios;

import java.util.List;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.PacketProtocolDecoder;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/** NetBIOS Name Service over UDP port 137. */
public class NetbiosNsDecoder implements PacketProtocolDecoder<NetbiosNsMessage> {
  private static final int PORT = 137;
  private static final String[] SECTIONS = {"answers", "authorities", "additionals"};

  @Override
  public String protocol() {
    return "netbios_ns";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("transaction_id", MinorType.INT)
        .addNullable("is_response", MinorType.BIT)
        .addNullable("opcode", MinorType.VARCHAR)
        .addNullable("rcode", MinorType.INT)
        .addNullable("broadcast", MinorType.BIT)
        .addMapArray("questions")
          .addNullable("name", MinorType.VARCHAR)
          .addNullable("suffix", MinorType.INT)
          .addNullable("suffix_name", MinorType.VARCHAR)
          .addNullable("type", MinorType.VARCHAR)
          .resumeSchema();
    for (String section : SECTIONS) {
      fields.addMapArray(section)
          .addNullable("name", MinorType.VARCHAR)
          .addNullable("suffix", MinorType.INT)
          .addNullable("suffix_name", MinorType.VARCHAR)
          .addNullable("type", MinorType.VARCHAR)
          .addNullable("ttl", MinorType.BIGINT)
          .addArray("addresses", MinorType.VARCHAR)
          .addNullable("node_type", MinorType.VARCHAR)
          .addNullable("is_group", MinorType.BIT)
          .addArray("names", MinorType.VARCHAR)
          .addNullable("mac_address", MinorType.VARCHAR)
          .resumeSchema();
    }
  }

  @Override
  public boolean accepts(Packet packet) {
    return packet.isUdpPacket() && (packet.getSrc_port() == PORT || packet.getDst_port() == PORT);
  }

  @Override
  public NetbiosNsMessage parse(Packet packet, byte[] payload, DecoderContext context) {
    return payload == null ? null : NetbiosNsParser.parse(payload, context);
  }

  @Override
  public void write(NetbiosNsMessage m, TupleWriter fields) {
    fields.scalar("transaction_id").setInt(m.transactionId);
    fields.scalar("is_response").setBoolean(m.isResponse);
    fields.scalar("opcode").setString(m.opcode);
    fields.scalar("rcode").setInt(m.rcode);
    fields.scalar("broadcast").setBoolean(m.broadcast);
    ArrayWriter questions = fields.array("questions");
    for (NetbiosNsMessage.Question q : m.questions) {
      TupleWriter t = questions.tuple();
      t.scalar("name").setString(q.name);
      t.scalar("suffix").setInt(q.suffix);
      if (q.suffixName != null) {
        t.scalar("suffix_name").setString(q.suffixName);
      }
      t.scalar("type").setString(q.type);
      questions.save();
    }
    List<?>[] lists = {m.answers, m.authorities, m.additionals};
    for (int s = 0; s < SECTIONS.length; s++) {
      ArrayWriter records = fields.array(SECTIONS[s]);
      for (Object o : lists[s]) {
        NetbiosNsMessage.Record r = (NetbiosNsMessage.Record) o;
        TupleWriter t = records.tuple();
        t.scalar("name").setString(r.name);
        t.scalar("suffix").setInt(r.suffix);
        if (r.suffixName != null) {
          t.scalar("suffix_name").setString(r.suffixName);
        }
        t.scalar("type").setString(r.type);
        t.scalar("ttl").setLong(r.ttl);
        if (r.addresses != null) {
          ArrayWriter addresses = t.array("addresses");
          for (String a : r.addresses) {
            addresses.scalar().setString(a);
          }
        }
        if (r.nodeType != null) {
          t.scalar("node_type").setString(r.nodeType);
        }
        if (r.group != null) {
          t.scalar("is_group").setBoolean(r.group);
        }
        if (r.names != null) {
          ArrayWriter names = t.array("names");
          for (String n : r.names) {
            names.scalar().setString(n);
          }
        }
        if (r.macAddress != null) {
          t.scalar("mac_address").setString(r.macAddress);
        }
        records.save();
      }
    }
  }
}
