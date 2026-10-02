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

import org.apache.drill.exec.vector.accessor.TupleWriter;

/** The decoder that handled a row, and its parsed value or the reason it failed. */
public final class DecodeResult {
  private final ProtocolDecoder decoder;
  private final Object parsed;
  private final String error;

  private DecodeResult(ProtocolDecoder decoder, Object parsed, String error) {
    this.decoder = decoder;
    this.parsed = parsed;
    this.error = error;
  }

  static DecodeResult success(ProtocolDecoder decoder, Object parsed) {
    return new DecodeResult(decoder, parsed, null);
  }

  static DecodeResult failure(ProtocolDecoder decoder, String error) {
    return new DecodeResult(decoder, null, error);
  }

  public String protocol() {
    return decoder.protocol();
  }

  /** Null if parsing failed. */
  public Object parsed() {
    return parsed;
  }

  /** Null if parsing succeeded. */
  public String error() {
    return error;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void write(TupleWriter fields) {
    ((PacketProtocolDecoder) decoder).write(parsed, fields);
  }
}
