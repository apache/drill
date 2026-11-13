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

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.MapBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/** Fields shared by the HTTP packet and session decoders. */
final class HttpFields {

  private HttpFields() { }

  static void setString(TupleWriter t, String name, String value) {
    if (value != null) {
      t.scalar(name).setString(value);
    }
  }

  static void writeRequest(HttpMessage m, TupleWriter t, String headersField) {
    setString(t, "method", m.method);
    setString(t, "uri", m.uri);
    setString(t, "version", m.version);
    setString(t, "host", m.header("Host"));
    setString(t, "user_agent", m.header("User-Agent"));
    setString(t, "referer", m.header("Referer"));
    setString(t, "username", m.username);
    t.scalar("password_present").setBoolean(m.passwordPresent);
    setString(t, "password", m.password);
    if (headersField.equals("headers")) {
      // Packet mode: a request body's type and length. In session mode these names hold the response's.
      setString(t, "content_type", m.header("Content-Type"));
      Long length = m.contentLength();
      if (length != null) {
        t.scalar("content_length").setLong(length);
      }
    }
    writeHeaders(m, t, headersField);
  }

  static void writeResponse(HttpMessage m, TupleWriter t, String headersField) {
    if (m.statusCode != null) {
      t.scalar("status_code").setInt(m.statusCode);
    }
    setString(t, "reason", m.reason);
    setString(t, "content_type", m.header("Content-Type"));
    Long length = m.contentLength();
    if (length != null) {
      t.scalar("content_length").setLong(length);
    }
    writeHeaders(m, t, headersField);
  }

  static void writeHeaders(HttpMessage m, TupleWriter t, String field) {
    ArrayWriter headers = t.array(field);
    for (String[] h : m.headers) {
      TupleWriter header = headers.tuple();
      header.scalar("name").setString(h[0]);
      header.scalar("value").setString(h[1]);
      headers.save();
    }
  }

  /** Fields of the packet decoder's sub-map. */
  static void definePacket(SchemaBuilder f) {
    f.addNullable("is_request", MinorType.BIT)
        .addNullable("method", MinorType.VARCHAR)
        .addNullable("uri", MinorType.VARCHAR)
        .addNullable("version", MinorType.VARCHAR)
        .addNullable("status_code", MinorType.INT)
        .addNullable("reason", MinorType.VARCHAR)
        .addNullable("host", MinorType.VARCHAR)
        .addNullable("user_agent", MinorType.VARCHAR)
        .addNullable("content_type", MinorType.VARCHAR)
        .addNullable("referer", MinorType.VARCHAR)
        .addNullable("content_length", MinorType.BIGINT)
        .addNullable("username", MinorType.VARCHAR)
        .addNullable("password_present", MinorType.BIT)
        .addNullable("password", MinorType.VARCHAR)
        .addMapArray("headers")
          .addNullable("name", MinorType.VARCHAR)
          .addNullable("value", MinorType.VARCHAR)
          .resumeSchema();
  }

  /** Fields of one entry of the session decoder's exchanges list. */
  static void defineExchange(MapBuilder e) {
    e.addNullable("method", MinorType.VARCHAR)
        .addNullable("uri", MinorType.VARCHAR)
        .addNullable("version", MinorType.VARCHAR)
        .addNullable("host", MinorType.VARCHAR)
        .addNullable("user_agent", MinorType.VARCHAR)
        .addNullable("referer", MinorType.VARCHAR)
        .addNullable("username", MinorType.VARCHAR)
        .addNullable("password_present", MinorType.BIT)
        .addNullable("password", MinorType.VARCHAR)
        .addMapArray("request_headers")
          .addNullable("name", MinorType.VARCHAR)
          .addNullable("value", MinorType.VARCHAR)
          .resumeMap()
        .addNullable("status_code", MinorType.INT)
        .addNullable("reason", MinorType.VARCHAR)
        .addNullable("content_type", MinorType.VARCHAR)
        .addNullable("content_length", MinorType.BIGINT)
        .addMapArray("response_headers")
          .addNullable("name", MinorType.VARCHAR)
          .addNullable("value", MinorType.VARCHAR)
          .resumeMap();
  }
}
