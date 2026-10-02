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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.drill.exec.record.metadata.MapBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.decoder.TcpSession;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.store.pcap.protocol.SessionProtocolDecoder;
import org.apache.drill.exec.store.pcap.protocol.TcpStream;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/** Every HTTP/1.x request and response of a TCP connection, paired in order. */
public class HttpSessionDecoder implements SessionProtocolDecoder<HttpExchanges> {
  static final int MAX_MESSAGES = 1000;

  @Override
  public String protocol() {
    return "http";
  }

  @Override
  public void defineSchema(SchemaBuilder fields) {
    MapBuilder exchanges = fields.addMapArray("exchanges");
    HttpFields.defineExchange(exchanges);
    exchanges.resumeSchema();
  }

  @Override
  public boolean accepts(TcpSession session) {
    return HttpParser.PORTS.contains(session.getSrcPort()) || HttpParser.PORTS.contains(session.getDstPort());
  }

  @Override
  public HttpExchanges parse(TcpStream fromClient, TcpStream fromServer, DecoderContext context) {
    HttpExchanges exchanges = new HttpExchanges();
    exchanges.requests.addAll(parseStream(fromClient.data(), true, Collections.emptyList(), context));
    if (exchanges.requests.isEmpty()) {
      return null;
    }
    exchanges.responses.addAll(parseStream(fromServer.data(), false, exchanges.requests, context));
    if (fromClient.hasGaps()) {
      context.warn("stopped at missing data in client stream at byte " + fromClient.firstGap());
    }
    if (fromServer.hasGaps()) {
      context.warn("stopped at missing data in server stream at byte " + fromServer.firstGap());
    }
    return exchanges;
  }

  /**
   * Parses consecutive messages.
   *
   * @param requestsForResponses for responses: the requests they answer, so a HEAD response has no body
   */
  static List<HttpMessage> parseStream(byte[] data, boolean requests, List<HttpMessage> requestsForResponses,
                                       DecoderContext context) {
    List<HttpMessage> messages = new ArrayList<>();
    String direction = requests ? "client" : "server";
    int offset = 0;
    while (offset < data.length && messages.size() < MAX_MESSAGES) {
      HttpMessage m = HttpParser.parseMessage(data, offset, data.length, context);
      if (m == null || m.isRequest != requests) {
        if (!messages.isEmpty()) {
          context.warn("unparseable data in " + direction + " stream at byte " + offset);
        }
        break;
      }
      // Interim responses (1xx) precede the final response to the same request
      boolean interim = !requests && m.statusCode != null && m.statusCode / 100 == 1;
      if (!interim) {
        messages.add(m);
      }
      if (m.headerEnd < 0) {
        break;
      }
      int index = interim ? messages.size() : messages.size() - 1;
      long bodyEnd = bodyEnd(m, data, requests ? null : request(requestsForResponses, index));
      if (bodyEnd < 0 || bodyEnd > data.length) {
        break; // body continues past the captured data
      }
      offset = (int) bodyEnd;
    }
    if (messages.size() == MAX_MESSAGES && offset < data.length) {
      context.warn("stopped after " + MAX_MESSAGES + " messages in " + direction + " stream");
    }
    return messages;
  }

  private static HttpMessage request(List<HttpMessage> requests, int index) {
    return index < requests.size() ? requests.get(index) : null;
  }

  /** Offset just past the message body, or -1 if it cannot be determined from the data. */
  private static long bodyEnd(HttpMessage m, byte[] data, HttpMessage request) {
    int start = m.headerEnd;
    if (!m.isRequest) {
      int code = m.statusCode;
      if ((request != null && "HEAD".equals(request.method)) || code / 100 == 1 || code == 204 || code == 304) {
        return start;
      }
    }
    if (m.isChunked()) {
      return chunkedEnd(data, start);
    }
    Long length = m.contentLength();
    if (length != null) {
      return start + length;
    }
    // A response without a length runs to the end of the connection; a request has no body
    return m.isRequest ? start : data.length;
  }

  private static long chunkedEnd(byte[] data, int start) {
    int p = start;
    while (true) {
      int lineEnd = HttpParser.lineEnd(data, p, data.length);
      if (lineEnd < 0) {
        return -1;
      }
      String sizeLine = new String(data, p, lineEnd - p, StandardCharsets.ISO_8859_1);
      int semicolon = sizeLine.indexOf(';');
      long size;
      try {
        size = Long.parseLong((semicolon >= 0 ? sizeLine.substring(0, semicolon) : sizeLine).trim(), 16);
      } catch (NumberFormatException e) {
        return -1;
      }
      p = lineEnd + 2;
      if (size == 0) {
        // Optional trailer lines, then a blank line
        while (true) {
          int end = HttpParser.lineEnd(data, p, data.length);
          if (end < 0) {
            return -1;
          }
          boolean empty = end == p;
          p = end + 2;
          if (empty) {
            return p;
          }
        }
      }
      if (size < 0 || p + size + 2 > data.length) {
        return -1;
      }
      p += (int) size + 2;
    }
  }

  @Override
  public void write(HttpExchanges parsed, TupleWriter fields) {
    ArrayWriter exchanges = fields.array("exchanges");
    int count = Math.max(parsed.requests.size(), parsed.responses.size());
    for (int i = 0; i < count; i++) {
      TupleWriter exchange = exchanges.tuple();
      if (i < parsed.requests.size()) {
        HttpFields.writeRequest(parsed.requests.get(i), exchange, "request_headers");
      }
      if (i < parsed.responses.size()) {
        HttpFields.writeResponse(parsed.responses.get(i), exchange, "response_headers");
      }
      exchanges.save();
    }
  }
}
