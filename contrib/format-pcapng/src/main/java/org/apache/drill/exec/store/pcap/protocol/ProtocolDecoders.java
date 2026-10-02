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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The protocol decoders found with {@link ServiceLoader}, and the matching rules of
 * docs/dev/PcapProtocolDecoders.md.
 */
public class ProtocolDecoders {
  private static final Logger logger = LoggerFactory.getLogger(ProtocolDecoders.class);
  private static volatile ProtocolDecoders instance;

  private final List<PacketProtocolDecoder<?>> packetDecoders;
  private final TupleMetadata packetDataSchema;

  /** The decoders on the classpath, loaded once. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static ProtocolDecoders get() {
    if (instance == null) {
      synchronized (ProtocolDecoders.class) {
        if (instance == null) {
          ClassLoader loader = ProtocolDecoders.class.getClassLoader();
          instance = new ProtocolDecoders((Iterable) ServiceLoader.load(PacketProtocolDecoder.class, loader));
        }
      }
    }
    return instance;
  }

  public ProtocolDecoders(Iterable<? extends PacketProtocolDecoder<?>> packetDecoders) {
    SchemaBuilder data = new SchemaBuilder();
    this.packetDecoders = usable(packetDecoders, data);
    this.packetDataSchema = data.buildSchema();
  }

  public List<PacketProtocolDecoder<?>> packetDecoders() {
    return packetDecoders;
  }

  /** Members of parsed_data in packet mode: one map per decoder. */
  public TupleMetadata packetDataSchema() {
    return packetDataSchema;
  }

  /**
   * Finds the decoder that handles a packet.
   *
   * @return null if no decoder handles it
   */
  public DecodeResult decodePacket(Packet packet, byte[] payload, RowDecoderContext context) {
    for (PacketProtocolDecoder<?> decoder : packetDecoders) {
      try {
        if (!decoder.accepts(packet)) {
          continue;
        }
      } catch (RuntimeException e) {
        // A pre-check that cannot evaluate the packet does not claim it
        continue;
      }
      context.begin(decoder.protocol());
      try {
        Object parsed = decoder.parse(packet, payload, context);
        if (parsed != null) {
          return DecodeResult.success(decoder, parsed);
        }
        context.discard();
      } catch (RuntimeException e) {
        context.discard();
        return DecodeResult.failure(decoder, decoder.protocol() + ": " + describe(e));
      }
    }
    return null;
  }

  public static String describe(Throwable e) {
    return e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
  }

  /**
   * Orders decoders by priority, drops duplicate protocol names and decoders whose schema
   * cannot be built, and adds each decoder's map to the parsed_data schema.
   */
  private static <D extends ProtocolDecoder> List<D> usable(Iterable<? extends D> decoders, SchemaBuilder data) {
    List<D> sorted = new ArrayList<>();
    decoders.forEach(sorted::add);
    sorted.sort(Comparator.comparingInt(ProtocolDecoder::priority).reversed());
    Map<String, D> byName = new HashMap<>();
    List<D> result = new ArrayList<>();
    for (D decoder : sorted) {
      D existing = byName.get(decoder.protocol());
      if (existing != null) {
        logger.warn("Ignoring protocol decoder {}: protocol {} is already handled by {}",
            decoder.getClass().getName(), decoder.protocol(), existing.getClass().getName());
        continue;
      }
      try {
        SchemaBuilder fields = new SchemaBuilder();
        defineSchema(decoder, fields);
        data.add(MetadataUtils.newMap(decoder.protocol(), fields.buildSchema()));
      } catch (RuntimeException e) {
        logger.warn("Ignoring protocol decoder {}: its schema could not be built", decoder.getClass().getName(), e);
        continue;
      }
      byName.put(decoder.protocol(), decoder);
      result.add(decoder);
    }
    return Collections.unmodifiableList(result);
  }

  private static void defineSchema(ProtocolDecoder decoder, SchemaBuilder fields) {
    ((PacketProtocolDecoder<?>) decoder).defineSchema(fields);
  }
}
