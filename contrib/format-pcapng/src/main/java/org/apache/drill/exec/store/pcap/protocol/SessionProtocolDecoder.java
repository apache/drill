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

import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.TcpSession;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * Decodes reassembled TCP sessions. Implementations are listed in
 * META-INF/services/org.apache.drill.exec.store.pcap.protocol.SessionProtocolDecoder.
 *
 * @param <T> the parsed form of one session
 */
public interface SessionProtocolDecoder<T> extends ProtocolDecoder {

  /** Declares the fields of parsed_data.&lt;protocol&gt; for session rows. */
  void defineSchema(SchemaBuilder fields);

  /** Cheap pre-check, typically ports. */
  boolean accepts(TcpSession session);

  /**
   * @return null if the session is not this protocol
   * @throws RuntimeException if the session is this protocol but malformed
   */
  T parse(TcpStream fromClient, TcpStream fromServer, DecoderContext context);

  void write(T parsed, TupleWriter fields);
}
