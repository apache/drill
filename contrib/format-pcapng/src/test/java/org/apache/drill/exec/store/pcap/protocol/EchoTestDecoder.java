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
package org.apache.drill.exec.store.pcap.protocol;

import java.nio.charset.StandardCharsets;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/** Test-only decoder for UDP port 7, registered through src/test/resources/META-INF/services. */
public class EchoTestDecoder implements PacketProtocolDecoder<String> {

  @Override
  public String protocol() {
    return "echo_test";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    fields.addNullable("text", MinorType.VARCHAR);
  }

  @Override
  public boolean accepts(Packet packet) {
    return packet.isUdpPacket() && packet.getDst_port() == 7;
  }

  @Override
  public String parse(Packet packet, byte[] payload, DecoderContext context) {
    String text = payload == null ? "" : new String(payload, StandardCharsets.US_ASCII);
    if (text.startsWith("ECHO!")) {
      throw new IllegalArgumentException("bad echo");
    }
    if (!text.startsWith("ECHO:")) {
      return null;
    }
    String value = text.substring(5);
    if (value.length() > 10) {
      context.warn("long text");
    }
    return value;
  }

  @Override
  public void write(String parsed, TupleWriter fields) {
    if (parsed.equals("write-fail")) {
      throw new IllegalStateException("cannot write");
    }
    fields.scalar("text").setString(parsed);
  }
}
