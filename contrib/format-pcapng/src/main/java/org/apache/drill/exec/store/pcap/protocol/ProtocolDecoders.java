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

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.decoder.TcpSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The protocol decoders found with {@link ServiceLoader}, and the matching rules of
 * docs/dev/PcapProtocolDecoders.md.
 */
public class ProtocolDecoders {
  private static final Logger logger = LoggerFactory.getLogger(ProtocolDecoders.class);
  private static volatile ProtocolDecoders instance;
  /** Initial width of VARCHAR fields in parsed_data, which most rows leave empty. */
  public static final int SPARSE_WIDTH = 8;

  private final List<PacketProtocolDecoder<?>> packetDecoders;
  private final TupleMetadata packetDataSchema;
  private final List<SessionProtocolDecoder<?>> sessionDecoders;
  private final TupleMetadata sessionDataSchema;

  /** The decoders on the classpath, loaded once. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static ProtocolDecoders get() {
    if (instance == null) {
      synchronized (ProtocolDecoders.class) {
        if (instance == null) {
          ClassLoader loader = ProtocolDecoders.class.getClassLoader();
          instance = new ProtocolDecoders((Iterable) ServiceLoader.load(PacketProtocolDecoder.class, loader),
              (Iterable) ServiceLoader.load(SessionProtocolDecoder.class, loader));
        }
      }
    }
    return instance;
  }

  public ProtocolDecoders(Iterable<? extends PacketProtocolDecoder<?>> packetDecoders) {
    this(packetDecoders, Collections.emptyList());
  }

  public ProtocolDecoders(Iterable<? extends PacketProtocolDecoder<?>> packetDecoders,
                          Iterable<? extends SessionProtocolDecoder<?>> sessionDecoders) {
    SchemaBuilder packetData = new SchemaBuilder();
    this.packetDecoders = usable(packetDecoders, packetData);
    this.packetDataSchema = packetData.buildSchema();
    SchemaBuilder sessionData = new SchemaBuilder();
    this.sessionDecoders = usable(sessionDecoders, sessionData);
    this.sessionDataSchema = sessionData.buildSchema();
  }

  public List<PacketProtocolDecoder<?>> packetDecoders() {
    return packetDecoders;
  }

  /** Members of parsed_data in packet mode: one map per decoder. */
  public TupleMetadata packetDataSchema() {
    return packetDataSchema;
  }

  public List<SessionProtocolDecoder<?>> sessionDecoders() {
    return sessionDecoders;
  }

  /** Members of parsed_data in session mode: one map per decoder. */
  public TupleMetadata sessionDataSchema() {
    return sessionDataSchema;
  }

  /**
   * Finds the decoder that handles a TCP session. The client and server streams are
   * reassembled only if some decoder accepts the session.
   *
   * @return null if no decoder handles it
   */
  public DecodeResult decodeSession(TcpSession session, RowDecoderContext context) {
    TcpStream[] streams = null;
    for (SessionProtocolDecoder<?> decoder : sessionDecoders) {
      try {
        if (!decoder.accepts(session)) {
          continue;
        }
      } catch (RuntimeException e) {
        continue;
      }
      context.begin(decoder.protocol());
      try {
        if (streams == null) {
          streams = TcpStream.clientServer(session);
        }
        Object parsed = decoder.parse(streams[0], streams[1], context);
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
        TupleMetadata schema = fields.buildSchema();
        sizeSparse(schema);
        data.add(MetadataUtils.newMap(decoder.protocol(), schema));
      } catch (RuntimeException e) {
        logger.warn("Ignoring protocol decoder {}: its schema could not be built", decoder.getClass().getName(), e);
        continue;
      }
      byName.put(decoder.protocol(), decoder);
      result.add(decoder);
    }
    return Collections.unmodifiableList(result);
  }

  /**
   * Starts every vector small. Drill otherwise reserves room for a full batch of
   * values per column, and ten elements per array row, which for the many mostly
   * empty decoder fields would use up the batch memory budget before any row is
   * written. Vectors still grow when a row needs more.
   */
  private static void sizeSparse(TupleMetadata schema) {
    for (ColumnMetadata column : schema) {
      if (column.isArray()) {
        column.setExpectedElementCount(1);
      }
      if (column.isMap()) {
        sizeSparse(column.tupleSchema());
      } else if (column.isVariableWidth()) {
        column.setExpectedWidth(SPARSE_WIDTH);
      }
    }
  }

  private static void defineSchema(ProtocolDecoder decoder, SchemaBuilder fields) {
    if (decoder instanceof SessionProtocolDecoder) {
      ((SessionProtocolDecoder<?>) decoder).defineSchema(fields);
    } else {
      ((PacketProtocolDecoder<?>) decoder).defineSchema(fields);
    }
  }
}
