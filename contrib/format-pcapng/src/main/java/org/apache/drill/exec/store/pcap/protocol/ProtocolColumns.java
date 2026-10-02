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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.decoder.TcpSession;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.slf4j.Logger;

/**
 * The parsed_protocol, parsed_data and decode_error columns, shared by the PCAP and PCAP-NG readers.
 */
public class ProtocolColumns {
  public static final String PARSED_PROTOCOL = "parsed_protocol";
  public static final String PARSED_DATA = "parsed_data";
  public static final String DECODE_ERROR = "decode_error";

  /** Packet rows, session rows, or stat rows (which only get decode_error). */
  public enum Mode { PACKET, SESSION, STAT }

  private final RowSetLoader loader;
  private final ProtocolDecoders decoders;
  private final RowDecoderContext context;
  private final ScalarWriter protocolWriter;
  private final TupleWriter dataWriter;
  private final ScalarWriter errorWriter;
  private final boolean decoding;
  // Errors written so far, by prefix (dns, packet, file, ...), for the end-of-file warning
  private final Map<String, Integer> errorCounts = new TreeMap<>();
  // Errors already written by writeErrorRowOnce
  private final Set<String> reportedErrors = new HashSet<>();

  public static void addColumns(SchemaBuilder schema, Mode mode, ProtocolDecoders decoders) {
    if (mode != Mode.STAT) {
      schema.addNullable(PARSED_PROTOCOL, MinorType.VARCHAR);
      schema.add(MetadataUtils.newMap(PARSED_DATA, dataSchema(mode, decoders)));
    }
    schema.addNullable(DECODE_ERROR, MinorType.VARCHAR);
  }

  private static TupleMetadata dataSchema(Mode mode, ProtocolDecoders decoders) {
    return mode == Mode.SESSION ? decoders.sessionDataSchema() : decoders.packetDataSchema();
  }

  public ProtocolColumns(RowSetLoader loader, Mode mode, ProtocolDecoders decoders, boolean exposeCredentials) {
    this.loader = loader;
    this.decoders = decoders;
    this.context = new RowDecoderContext(exposeCredentials);
    this.errorWriter = loader.scalar(DECODE_ERROR);
    if (mode == Mode.STAT) {
      protocolWriter = null;
      dataWriter = null;
      decoding = false;
    } else {
      protocolWriter = loader.scalar(PARSED_PROTOCOL);
      dataWriter = loader.tuple(PARSED_DATA);
      decoding = loader.isProjected(PARSED_PROTOCOL) || loader.isProjected(PARSED_DATA);
    }
  }

  /** True if parsed_protocol or parsed_data is projected, so decoding is worth doing. */
  public boolean isDecoding() {
    return decoding;
  }

  /**
   * Decodes a packet into the current row and writes decode_error.
   *
   * @param packet the decoded packet; null if it could not be decoded at all
   * @param errors problems the reader already found for this row; may be null
   */
  public void writePacket(Packet packet, List<String> errors) {
    List<String> all = errors == null ? new ArrayList<>() : new ArrayList<>(errors);
    if (decoding && packet != null) {
      context.reset();
      byte[] payload = null;
      try {
        payload = packet.getData();
      } catch (RuntimeException e) {
        all.add("packet: " + ProtocolDecoders.describe(e));
      }
      write(decoders.decodePacket(packet, payload, context), all);
    }
    writeErrors(all);
  }

  /** Decodes a session into the current row and writes decode_error. */
  public void writeSession(TcpSession session) {
    List<String> all = new ArrayList<>();
    if (decoding) {
      context.reset();
      write(decoders.decodeSession(session, context), all);
    }
    writeErrors(all);
  }

  void write(DecodeResult result, List<String> all) {
    if (result != null) {
      protocolWriter.setString(result.protocol());
      if (result.parsed() != null) {
        try {
          result.write(dataWriter.tuple(result.protocol()));
        } catch (RuntimeException e) {
          all.add(result.protocol() + ": write failed: " + ProtocolDecoders.describe(e));
        }
      }
      if (result.error() != null) {
        all.add(result.error());
      }
    }
    all.addAll(context.warnings());
  }

  /** Sets decode_error on the current row; does nothing for an empty list. */
  public void writeErrors(List<String> errors) {
    if (errors != null && !errors.isEmpty()) {
      errorWriter.setString(String.join("; ", errors));
      errors.forEach(this::count);
    }
  }

  /** Writes a row with only decode_error set. */
  public void writeErrorRow(String error) {
    loader.start();
    errorWriter.setString(error);
    loader.save();
    count(error);
  }

  /**
   * Writes an error row unless the same error was already written. Used where
   * packets are not rows (session mode), so each distinct problem appears once.
   *
   * @return true if a row was written
   */
  public boolean writeErrorRowOnce(String error) {
    if (!reportedErrors.add(error)) {
      count(error);
      return false;
    }
    writeErrorRow(error);
    return true;
  }

  private void count(String error) {
    int colon = error.indexOf(':');
    errorCounts.merge(colon > 0 ? error.substring(0, colon) : error, 1, Integer::sum);
  }

  /** Logs one warning for the file if any row had an error, such as "{dns=3, packet=1}". */
  public void logSummary(Logger logger, String file) {
    if (!errorCounts.isEmpty()) {
      logger.warn("{}: rows with decode_error by kind: {}", file, errorCounts);
    }
  }
}
