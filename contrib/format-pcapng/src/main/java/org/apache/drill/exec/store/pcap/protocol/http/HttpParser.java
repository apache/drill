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
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;

/** Parses HTTP/1.x start lines and headers with bounded work. */
public final class HttpParser {
  public static final Set<Integer> PORTS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(80, 591, 3128, 8000, 8008, 8080, 8888)));
  static final int MAX_HEADERS = 64;
  static final int MAX_STRING = 4096;
  private static final int MAX_LINE = 8192;
  private static final Pattern REQUEST =
      Pattern.compile("(GET|POST|PUT|DELETE|HEAD|OPTIONS|PATCH|CONNECT|TRACE) (\\S+) HTTP/(1\\.[01])");
  private static final Pattern STATUS = Pattern.compile("HTTP/(1\\.[01]) ([1-5]\\d\\d)(?: (.*))?");
  private static final Pattern HEADER = Pattern.compile("([!#$%&'*+.^_`|~0-9A-Za-z-]+):[ \\t]*(.*?)[ \\t]*");

  private HttpParser() { }

  /**
   * @return the message at offset, or null if no request or status line starts there
   */
  public static HttpMessage parseMessage(byte[] data, int offset, int limit, DecoderContext context) {
    int lineEnd = lineEnd(data, offset, limit);
    if (lineEnd < 0) {
      return null;
    }
    String startLine = new String(data, offset, lineEnd - offset, StandardCharsets.ISO_8859_1);
    HttpMessage m = new HttpMessage();
    m.setContext(context);
    Matcher request = REQUEST.matcher(startLine);
    Matcher status = STATUS.matcher(startLine);
    if (request.matches()) {
      m.isRequest = true;
      m.method = request.group(1);
      m.uri = cap(request.group(2));
      m.version = request.group(3);
    } else if (status.matches()) {
      m.version = status.group(1);
      m.statusCode = Integer.parseInt(status.group(2));
      m.reason = status.group(3) == null ? null : cap(status.group(3));
    } else {
      return null;
    }
    int p = lineEnd + 2;
    boolean capped = false;
    while (true) {
      int end = lineEnd(data, p, limit);
      if (end < 0) {
        break; // headers continue in a later segment; headerEnd stays -1
      }
      if (end == p) {
        m.headerEnd = p + 2;
        break;
      }
      String line = new String(data, p, end - p, StandardCharsets.ISO_8859_1);
      Matcher header = HEADER.matcher(line);
      if (!header.matches()) {
        context.warn("malformed header line");
        break;
      }
      if (m.headers.size() < MAX_HEADERS) {
        m.headers.add(new String[] {header.group(1), cap(header.group(2))});
      } else if (!capped) {
        context.warn("headers truncated to " + MAX_HEADERS);
        capped = true;
      }
      p = end + 2;
    }
    credentials(m, context);
    return m;
  }

  private static void credentials(HttpMessage m, DecoderContext context) {
    String auth = m.header("Authorization");
    if (auth == null || !auth.regionMatches(true, 0, "Basic ", 0, 6)) {
      return;
    }
    try {
      String decoded = new String(Base64.getDecoder().decode(auth.substring(6).trim()), StandardCharsets.UTF_8);
      int colon = decoded.indexOf(':');
      if (colon < 0) {
        context.warn("Basic credentials without a colon");
        return;
      }
      m.username = cap(decoded.substring(0, colon));
      m.passwordPresent = true;
      if (context.exposeCredentials()) {
        m.password = cap(decoded.substring(colon + 1));
      }
    } catch (IllegalArgumentException e) {
      context.warn("invalid Basic credentials");
    }
  }

  /** Offset of the CR of the next CRLF, or -1 if there is none within MAX_LINE bytes. */
  static int lineEnd(byte[] data, int from, int limit) {
    int max = Math.min(limit, from + MAX_LINE);
    for (int i = from; i + 1 < max; i++) {
      if (data[i] == '\r' && data[i + 1] == '\n') {
        return i;
      }
    }
    return -1;
  }

  static String cap(String s) {
    return s.length() > MAX_STRING ? s.substring(0, MAX_STRING) : s;
  }
}
