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
package org.apache.drill.exec.store.pcap.protocol.http;

import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.PacketProtocolDecoder;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/** The HTTP/1.x request or response that starts in a TCP segment. */
public class HttpPacketDecoder implements PacketProtocolDecoder<HttpMessage> {

  @Override
  public String protocol() {
    return "http";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    HttpFields.definePacket(fields);
  }

  @Override
  public boolean accepts(Packet packet) {
    return packet.isTcpPacket()
        && (HttpParser.PORTS.contains(packet.getSrc_port()) || HttpParser.PORTS.contains(packet.getDst_port()));
  }

  @Override
  public HttpMessage parse(Packet packet, byte[] payload, DecoderContext context) {
    return payload == null ? null : HttpParser.parseMessage(payload, 0, payload.length, context);
  }

  @Override
  public void write(HttpMessage m, TupleWriter fields) {
    fields.scalar("is_request").setBoolean(m.isRequest);
    if (m.isRequest) {
      HttpFields.writeRequest(m, fields, "headers");
    } else {
      HttpFields.writeResponse(m, fields, "headers");
    }
  }
}
