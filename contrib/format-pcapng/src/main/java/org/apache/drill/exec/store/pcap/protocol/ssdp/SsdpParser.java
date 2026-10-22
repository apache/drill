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
package org.apache.drill.exec.store.pcap.protocol.ssdp;

import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;

/**
 * Parses SSDP (HTTP over UDP, as used by UPnP discovery). Lines may end with CRLF or a bare LF,
 * which some devices send.
 */
public final class SsdpParser {
  static final int MAX_HEADERS = 64;
  static final int MAX_STRING = 4096;
  private static final Pattern REQUEST = Pattern.compile("(M-SEARCH|NOTIFY) \\* HTTP/1\\.[01]");
  private static final Pattern STATUS = Pattern.compile("HTTP/1\\.[01] ([1-5]\\d\\d)(?: .*)?");
  private static final Pattern HEADER = Pattern.compile("([!#$%&'*+.^_`|~0-9A-Za-z-]+):[ \\t]*(.*?)[ \\t]*");

  private SsdpParser() { }

  /**
   * @return the message, or null if the data does not start with an SSDP start line
   * @throws IllegalArgumentException if a header line is malformed
   */
  public static SsdpMessage parse(byte[] data, DecoderContext context) {
    if (data == null || data.length == 0) {
      return null;
    }
    String text = new String(data, StandardCharsets.UTF_8);
    int lineEnd = text.indexOf('\n');
    String startLine = stripCr(lineEnd < 0 ? text : text.substring(0, lineEnd));
    SsdpMessage m = new SsdpMessage();
    Matcher request = REQUEST.matcher(startLine);
    Matcher status = STATUS.matcher(startLine);
    if (request.matches()) {
      m.method = request.group(1);
    } else if (status.matches()) {
      m.statusCode = Integer.parseInt(status.group(1));
    } else {
      return null;
    }
    boolean capped = false;
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
      Matcher header = HEADER.matcher(line);
      if (!header.matches()) {
        if (lineEnd < 0) {
          break; // the capture cut the last line short
        }
        throw new IllegalArgumentException("malformed header line " + lineNumber);
      }
      String name = header.group(1);
      String value = cap(header.group(2));
      if (m.headers.size() < MAX_HEADERS) {
        m.headers.add(new String[] {name, value});
      } else if (!capped) {
        context.warn("headers truncated to " + MAX_HEADERS);
        capped = true;
      }
      field(m, name, value, context);
    }
    return m;
  }

  /** Sets the named field for a header, keeping the first occurrence. */
  private static void field(SsdpMessage m, String name, String value, DecoderContext context) {
    switch (name.toUpperCase()) {
      case "ST":
        m.st = m.st == null ? value : m.st;
        break;
      case "NT":
        m.nt = m.nt == null ? value : m.nt;
        break;
      case "NTS":
        m.nts = m.nts == null ? value : m.nts;
        break;
      case "USN":
        m.usn = m.usn == null ? value : m.usn;
        break;
      case "LOCATION":
        m.location = m.location == null ? value : m.location;
        break;
      case "SERVER":
        m.server = m.server == null ? value : m.server;
        break;
      case "USER-AGENT":
        m.userAgent = m.userAgent == null ? value : m.userAgent;
        break;
      case "MAN":
        m.man = m.man == null ? value : m.man;
        break;
      case "CACHE-CONTROL":
        m.cacheControl = m.cacheControl == null ? value : m.cacheControl;
        break;
      case "MX":
        if (m.mx == null) {
          try {
            m.mx = Integer.parseInt(value);
          } catch (NumberFormatException e) {
            context.warn("invalid MX " + (value.length() > 32 ? value.substring(0, 32) : value));
          }
        }
        break;
      default:
        break;
    }
  }

  private static String stripCr(String line) {
    return line.endsWith("\r") ? line.substring(0, line.length() - 1) : line;
  }

  private static String cap(String s) {
    return s.length() > MAX_STRING ? s.substring(0, MAX_STRING) : s;
  }
}
