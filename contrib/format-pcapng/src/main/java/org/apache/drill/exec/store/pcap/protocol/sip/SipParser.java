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
package org.apache.drill.exec.store.pcap.protocol.sip;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;

/**
 * Parses the start line and headers of a SIP message (RFC 3261), including compact header
 * names and folded header lines. The body is not returned.
 */
public final class SipParser {
  static final int MAX_HEADERS = 64;
  static final int MAX_STRING = 4096;
  private static final Pattern REQUEST = Pattern.compile(
      "(INVITE|ACK|BYE|CANCEL|REGISTER|OPTIONS|PRACK|SUBSCRIBE|NOTIFY|PUBLISH|INFO|REFER|MESSAGE|UPDATE) (\\S+) SIP/2\\.0");
  private static final Pattern STATUS = Pattern.compile("SIP/2\\.0 ([1-6]\\d\\d)(?: (.*))?");
  private static final Pattern HEADER = Pattern.compile("([!%'*+.^_`~0-9A-Za-z-]+)[ \\t]*:[ \\t]*(.*?)[ \\t]*");
  private static final Pattern USERNAME =
      Pattern.compile("(?:^|[\\s,])username[ \\t]*=[ \\t]*(?:\"((?:[^\"\\\\]|\\\\.)*)\"|([^\\s,]+))",
          Pattern.CASE_INSENSITIVE);
  private static final Map<String, String> COMPACT = new HashMap<>();

  static {
    COMPACT.put("i", "call-id");
    COMPACT.put("m", "contact");
    COMPACT.put("e", "content-encoding");
    COMPACT.put("l", "content-length");
    COMPACT.put("c", "content-type");
    COMPACT.put("f", "from");
    COMPACT.put("s", "subject");
    COMPACT.put("k", "supported");
    COMPACT.put("t", "to");
    COMPACT.put("v", "via");
  }

  private SipParser() { }

  /**
   * @return the message, or null if the data does not start with a SIP request or status line
   * @throws IllegalArgumentException if a header line is malformed
   */
  public static SipMessage parse(byte[] data, DecoderContext context) {
    if (data == null || data.length == 0) {
      return null;
    }
    String text = new String(data, StandardCharsets.UTF_8);
    int lineEnd = text.indexOf('\n');
    String startLine = stripCr(lineEnd < 0 ? text : text.substring(0, lineEnd));
    SipMessage m = new SipMessage();
    Matcher request = REQUEST.matcher(startLine);
    Matcher status = STATUS.matcher(startLine);
    if (request.matches()) {
      m.isRequest = true;
      m.method = request.group(1);
      m.requestUri = cap(request.group(2));
    } else if (status.matches()) {
      m.statusCode = Integer.parseInt(status.group(1));
      m.reason = status.group(2) == null ? null : cap(status.group(2));
    } else {
      return null;
    }
    State state = new State(m, context);
    int lineNumber = 0;
    int p = lineEnd + 1;
    while (lineEnd >= 0 && p < text.length()) {
      lineEnd = text.indexOf('\n', p);
      String line = stripCr(lineEnd < 0 ? text.substring(p) : text.substring(p, lineEnd));
      p = lineEnd + 1;
      lineNumber++;
      if (line.isEmpty()) {
        break;
      }
      if ((line.charAt(0) == ' ' || line.charAt(0) == '\t') && state.name != null) {
        state.value.append(' ').append(line.trim());
        continue;
      }
      Matcher header = HEADER.matcher(line);
      if (!header.matches()) {
        if (lineEnd < 0) {
          break; // the capture cut the last line short
        }
        throw new IllegalArgumentException("malformed header line " + lineNumber);
      }
      state.finish();
      state.name = header.group(1);
      state.value.append(header.group(2));
    }
    state.finish();
    return m;
  }

  /** The header being read, which folded lines may still extend. */
  private static final class State {
    final SipMessage m;
    final DecoderContext context;
    final StringBuilder value = new StringBuilder();
    String name;
    boolean headersCapped;
    boolean viaCapped;

    State(SipMessage m, DecoderContext context) {
      this.m = m;
      this.context = context;
    }

    void finish() {
      if (name == null) {
        return;
      }
      String v = cap(value.toString());
      if (m.headers.size() < MAX_HEADERS) {
        m.headers.add(new String[] {name, v});
      } else if (!headersCapped) {
        context.warn("headers truncated to " + MAX_HEADERS);
        headersCapped = true;
      }
      field(name.toLowerCase(), v);
      name = null;
      value.setLength(0);
    }

    private void field(String lower, String v) {
      String canonical = COMPACT.getOrDefault(lower, lower);
      switch (canonical) {
        case "from":
          m.from = m.from == null ? v : m.from;
          break;
        case "to":
          m.to = m.to == null ? v : m.to;
          break;
        case "call-id":
          m.callId = m.callId == null ? v : m.callId;
          break;
        case "cseq":
          m.cseq = m.cseq == null ? v : m.cseq;
          break;
        case "user-agent":
          m.userAgent = m.userAgent == null ? v : m.userAgent;
          break;
        case "contact":
          m.contact = m.contact == null ? v : m.contact;
          break;
        case "content-type":
          m.contentType = m.contentType == null ? v : m.contentType;
          break;
        case "content-length":
          contentLength(v);
          break;
        case "via":
          via(v);
          break;
        case "authorization":
        case "proxy-authorization":
          username(v);
          break;
        default:
          break;
      }
    }

    private void contentLength(String v) {
      if (m.contentLength != null) {
        return;
      }
      try {
        long length = Long.parseLong(v);
        if (length >= 0) {
          m.contentLength = length;
          return;
        }
      } catch (NumberFormatException e) {
        // reported below
      }
      context.warn("invalid Content-Length " + (v.length() > 32 ? v.substring(0, 32) : v));
    }

    /** Splits a Via header on commas outside quotes and angle brackets. */
    private void via(String v) {
      boolean quoted = false;
      int depth = 0;
      int start = 0;
      for (int i = 0; i <= v.length(); i++) {
        char c = i < v.length() ? v.charAt(i) : ',';
        if (c == '"') {
          quoted = !quoted;
        } else if (!quoted && c == '<') {
          depth++;
        } else if (!quoted && c == '>' && depth > 0) {
          depth--;
        } else if ((!quoted && depth == 0 && c == ',') || i == v.length()) {
          String part = v.substring(start, i).trim();
          start = i + 1;
          if (part.isEmpty()) {
            continue;
          }
          if (m.via.size() < MAX_HEADERS) {
            m.via.add(part);
          } else if (!viaCapped) {
            context.warn("via truncated to " + MAX_HEADERS);
            viaCapped = true;
          }
        }
      }
    }

    private void username(String v) {
      if (m.username != null || !v.regionMatches(true, 0, "Digest", 0, 6)) {
        return;
      }
      Matcher u = USERNAME.matcher(v.substring(6));
      if (u.find()) {
        m.username = cap(u.group(1) != null ? u.group(1).replaceAll("\\\\(.)", "$1") : u.group(2));
        m.passwordPresent = false;
      }
    }
  }

  private static String stripCr(String line) {
    return line.endsWith("\r") ? line.substring(0, line.length() - 1) : line;
  }

  static String cap(String s) {
    return s.length() > MAX_STRING ? s.substring(0, MAX_STRING) : s;
  }
}
