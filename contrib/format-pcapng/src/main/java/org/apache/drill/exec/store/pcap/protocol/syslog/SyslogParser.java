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
package org.apache.drill.exec.store.pcap.protocol.syslog;

import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;

/**
 * Parses syslog messages: RFC 5424 ({@code <PRI>1 TIMESTAMP HOST APP PROCID MSGID SD MSG}),
 * RFC 3164 ({@code <PRI>Mmm dd hh:mm:ss HOST TAG: MSG}), and messages that have only a priority.
 */
public final class SyslogParser {
  static final int MAX_STRING = 4096;
  private static final int MAX_PRI = 191;
  private static final String[] FACILITIES = {
      "kern", "user", "mail", "daemon", "auth", "syslog", "lpr", "news", "uucp", "cron", "authpriv", "ftp", "ntp",
      "audit", "alert", "clock", "local0", "local1", "local2", "local3", "local4", "local5", "local6", "local7"};
  private static final String[] SEVERITIES = {"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"};
  private static final Pattern VERSION = Pattern.compile("([1-9]\\d?) ");
  private static final Pattern BSD_TIMESTAMP = Pattern.compile("[A-Z][a-z]{2} [ \\d]\\d \\d\\d:\\d\\d:\\d\\d ");
  private static final Pattern TAG = Pattern.compile("([^\\s\\[\\]:]{1,48})(?:\\[([^\\]\\s]{0,32})\\])?:(?: ?(.*))?",
      Pattern.DOTALL);
  private static final int BSD_TIMESTAMP_LENGTH = 15;
  private static final int RFC5424_HEADER_FIELDS = 5;

  private SyslogParser() { }

  /**
   * @return the message, or null if the data does not start with a syslog priority
   * @throws IllegalArgumentException if an RFC 5424 header is incomplete or its structured data is invalid
   */
  public static SyslogMessage parse(byte[] data, DecoderContext context) {
    if (data == null || data.length < 3 || data[0] != '<') {
      return null;
    }
    int pri = 0;
    int i = 1;
    while (i < data.length && i <= 3 && data[i] >= '0' && data[i] <= '9') {
      pri = pri * 10 + (data[i] - '0');
      i++;
    }
    if (i == 1 || i >= data.length || data[i] != '>' || pri > MAX_PRI) {
      return null;
    }
    SyslogMessage m = new SyslogMessage();
    m.facility = pri >> 3;
    m.facilityName = FACILITIES[m.facility];
    m.severity = pri & 0x07;
    m.severityName = SEVERITIES[m.severity];
    String rest = stripEnd(new String(data, i + 1, data.length - i - 1, StandardCharsets.UTF_8));
    Matcher version = VERSION.matcher(rest);
    if (version.lookingAt()) {
      m.version = Integer.parseInt(version.group(1));
      parse5424(rest.substring(version.end()), m, context);
    } else if (BSD_TIMESTAMP.matcher(rest).lookingAt()) {
      m.timestampText = rest.substring(0, BSD_TIMESTAMP_LENGTH);
      parse3164(rest.substring(BSD_TIMESTAMP_LENGTH + 1), m, context);
    } else {
      m.message = message(rest, context);
    }
    return m;
  }

  private static void parse5424(String s, SyslogMessage m, DecoderContext context) {
    String[] header = new String[RFC5424_HEADER_FIELDS];
    int p = 0;
    for (int f = 0; f < RFC5424_HEADER_FIELDS; f++) {
      int space = s.indexOf(' ', p);
      if (space < 0) {
        throw new IllegalArgumentException("truncated RFC 5424 header");
      }
      header[f] = nil(s.substring(p, space), context);
      p = space + 1;
    }
    m.timestampText = header[0];
    m.hostname = header[1];
    m.appName = header[2];
    m.procId = header[3];
    m.msgId = header[4];
    if (p >= s.length()) {
      throw new IllegalArgumentException("truncated RFC 5424 header: no structured data");
    }
    int sdStart = p;
    if (s.charAt(p) == '-') {
      p++;
    } else {
      while (p < s.length() && s.charAt(p) == '[') {
        p = elementEnd(s, p) + 1;
      }
      if (p == sdStart) {
        throw new IllegalArgumentException("invalid structured data");
      }
      m.structuredData = cap(s.substring(sdStart, p), "structured data", context);
    }
    if (p < s.length()) {
      if (s.charAt(p) != ' ') {
        throw new IllegalArgumentException("invalid structured data");
      }
      m.message = message(s.substring(p + 1), context);
    }
  }

  /** Index of the ']' closing the SD-ELEMENT that starts at start, skipping quoted and escaped characters. */
  private static int elementEnd(String s, int start) {
    boolean quoted = false;
    for (int j = start + 1; j < s.length(); j++) {
      char c = s.charAt(j);
      if (quoted && c == '\\') {
        j++;
      } else if (c == '"') {
        quoted = !quoted;
      } else if (!quoted && c == ']') {
        return j;
      }
    }
    throw new IllegalArgumentException("unterminated structured data");
  }

  private static void parse3164(String s, SyslogMessage m, DecoderContext context) {
    int space = s.indexOf(' ');
    if (space > 0 && !TAG.matcher(s.substring(0, space)).matches()) {
      m.hostname = cap(s.substring(0, space), "hostname", context);
      s = s.substring(space + 1);
    }
    Matcher tag = TAG.matcher(s);
    if (tag.matches()) {
      m.appName = tag.group(1);
      m.procId = tag.group(2) == null || tag.group(2).isEmpty() ? null : tag.group(2);
      m.message = tag.group(3) == null ? null : message(tag.group(3), context);
    } else {
      m.message = message(s, context);
    }
  }

  private static String nil(String value, DecoderContext context) {
    return value.equals("-") || value.isEmpty() ? null : cap(value, "header field", context);
  }

  private static String message(String s, DecoderContext context) {
    if (s.startsWith("\uFEFF")) {
      s = s.substring(1);
    }
    return s.isEmpty() ? null : cap(s, "message", context);
  }

  private static String stripEnd(String s) {
    int end = s.length();
    while (end > 0 && (s.charAt(end - 1) == '\n' || s.charAt(end - 1) == '\r' || s.charAt(end - 1) == '\0')) {
      end--;
    }
    return s.substring(0, end);
  }

  private static String cap(String s, String what, DecoderContext context) {
    if (s.length() <= MAX_STRING) {
      return s;
    }
    context.warn(what + " truncated to " + MAX_STRING + " characters");
    return s.substring(0, MAX_STRING);
  }
}
