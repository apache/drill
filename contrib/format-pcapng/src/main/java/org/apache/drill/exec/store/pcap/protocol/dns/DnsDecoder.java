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
package org.apache.drill.exec.store.pcap.protocol.dns;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.PacketProtocolDecoder;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/** DNS, mDNS and LLMNR over UDP, and single-segment DNS over TCP. */
public class DnsDecoder implements PacketProtocolDecoder<DnsMessage> {
  private static final Set<Integer> PORTS = new HashSet<>(Arrays.asList(53, 5353, 5355));
  private static final String[] SECTIONS = {"answers", "authorities", "additionals"};

  @Override
  public String protocol() {
    return "dns";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("transaction_id", MinorType.INT)
        .addNullable("is_response", MinorType.BIT)
        .addNullable("opcode", MinorType.INT)
        .addNullable("rcode", MinorType.INT)
        .addNullable("authoritative", MinorType.BIT)
        .addNullable("truncated", MinorType.BIT)
        .addNullable("recursion_desired", MinorType.BIT)
        .addNullable("recursion_available", MinorType.BIT)
        .addMapArray("questions")
          .addNullable("name", MinorType.VARCHAR)
          .addNullable("type", MinorType.VARCHAR)
          .addNullable("class", MinorType.INT)
          .resumeSchema();
    for (String section : SECTIONS) {
      fields.addMapArray(section)
          .addNullable("name", MinorType.VARCHAR)
          .addNullable("type", MinorType.VARCHAR)
          .addNullable("class", MinorType.INT)
          .addNullable("ttl", MinorType.BIGINT)
          .addNullable("data", MinorType.VARCHAR)
          .resumeSchema();
    }
  }

  @Override
  public boolean accepts(Packet packet) {
    return (packet.isUdpPacket() || packet.isTcpPacket())
        && (PORTS.contains(packet.getSrc_port()) || PORTS.contains(packet.getDst_port()));
  }

  @Override
  public DnsMessage parse(Packet packet, byte[] payload, DecoderContext context) {
    if (payload == null) {
      return null;
    }
    int offset = 0;
    if (packet.isTcpPacket()) {
      // DNS over TCP starts with a 2-byte length; only whole messages in one segment are handled here
      if (payload.length < 2 || (((payload[0] & 0xFF) << 8) | (payload[1] & 0xFF)) != payload.length - 2) {
        return null;
      }
      offset = 2;
    }
    return DnsParser.parse(payload, offset, context);
  }

  @Override
  public void write(DnsMessage m, TupleWriter fields) {
    fields.scalar("transaction_id").setInt(m.transactionId);
    fields.scalar("is_response").setBoolean(m.isResponse);
    fields.scalar("opcode").setInt(m.opcode);
    fields.scalar("rcode").setInt(m.rcode);
    fields.scalar("authoritative").setBoolean(m.authoritative);
    fields.scalar("truncated").setBoolean(m.truncated);
    fields.scalar("recursion_desired").setBoolean(m.recursionDesired);
    fields.scalar("recursion_available").setBoolean(m.recursionAvailable);
    ArrayWriter questions = fields.array("questions");
    for (DnsMessage.Question q : m.questions) {
      TupleWriter t = questions.tuple();
      t.scalar("name").setString(q.name);
      t.scalar("type").setString(q.type);
      t.scalar("class").setInt(q.dnsClass);
      questions.save();
    }
    List<?>[] lists = {m.answers, m.authorities, m.additionals};
    for (int s = 0; s < SECTIONS.length; s++) {
      ArrayWriter records = fields.array(SECTIONS[s]);
      for (Object o : lists[s]) {
        DnsMessage.Record r = (DnsMessage.Record) o;
        TupleWriter t = records.tuple();
        t.scalar("name").setString(r.name);
        t.scalar("type").setString(r.type);
        t.scalar("class").setInt(r.dnsClass);
        t.scalar("ttl").setLong(r.ttl);
        t.scalar("data").setString(r.data);
        records.save();
      }
    }
  }
}
