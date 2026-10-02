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
package org.apache.drill.exec.store.pcap.protocol.mail;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The main headers of an RFC 5322 message (From, To, Cc, Subject, Date, Message-ID). Folded lines are
 * unfolded and RFC 2047 encoded-words in UTF-8 (or US-ASCII) are decoded; other charsets keep the raw text.
 */
public final class MailHeaders {
  /** Header section bytes examined per message. */
  public static final int MAX_HEADER_BYTES = 65536;
  private static final int MAX_RAW_VALUE = 4 * MailLines.MAX_STRING;
  private static final Pattern ENCODED_WORD = Pattern.compile("=\\?([^?\\s]+)\\?([BbQq])\\?([^?\\s]*)\\?=");
  private static final Pattern BETWEEN_WORDS = Pattern.compile("\\?=\\s+=\\?");

  public String from;
  public String to;
  public String cc;
  public String subject;
  public String date;
  public String messageId;

  /** Parses the header section at the start of data[start, end), up to the first empty line. */
  public static MailHeaders parse(byte[] data, int start, int end) {
    MailHeaders headers = new MailHeaders();
    int limit = end - start > MAX_HEADER_BYTES ? start + MAX_HEADER_BYTES : end;
    String name = null;
    StringBuilder value = null;
    int p = start;
    while (p < limit) {
      int lf = MailLines.lineEnd(data, p, limit);
      int lineEnd = lf < 0 ? limit : lf;
      String line = MailLines.text(data, p, lineEnd, MAX_RAW_VALUE);
      p = lineEnd + 1;
      if (line.isEmpty()) {
        break;
      }
      char first = line.charAt(0);
      if (first == ' ' || first == '\t') {
        if (value != null && value.length() < MAX_RAW_VALUE) {
          value.append(line);
        }
        continue;
      }
      headers.set(name, value);
      int colon = line.indexOf(':');
      if (colon <= 0) {
        name = null;
        value = null;
      } else {
        name = line.substring(0, colon).trim().toLowerCase(Locale.ROOT);
        value = new StringBuilder(line.substring(colon + 1));
      }
    }
    headers.set(name, value);
    return headers;
  }

  /** Parses a header section held in a stream of header bytes. */
  public static MailHeaders parse(ByteArrayOutputStream section) {
    byte[] bytes = section.toByteArray();
    return parse(bytes, 0, bytes.length);
  }

  private void set(String name, StringBuilder raw) {
    if (name == null) {
      return;
    }
    switch (name) {
      case "from":
        from = from != null ? from : value(raw);
        break;
      case "to":
        to = to != null ? to : value(raw);
        break;
      case "cc":
        cc = cc != null ? cc : value(raw);
        break;
      case "subject":
        subject = subject != null ? subject : value(raw);
        break;
      case "date":
        date = date != null ? date : value(raw);
        break;
      case "message-id":
        messageId = messageId != null ? messageId : value(raw);
        break;
      default:
        break;
    }
  }

  private static String value(StringBuilder raw) {
    String s = raw.length() > MAX_RAW_VALUE ? raw.substring(0, MAX_RAW_VALUE) : raw.toString();
    return MailLines.cap(decodeWords(s.trim()));
  }

  /** Decodes RFC 2047 encoded-words in UTF-8 or US-ASCII; anything else is left as it is. */
  public static String decodeWords(String s) {
    if (s == null || !s.contains("=?")) {
      return s;
    }
    // Whitespace between two adjacent encoded-words is not part of the text
    String joined = BETWEEN_WORDS.matcher(s).replaceAll("?==?");
    Matcher m = ENCODED_WORD.matcher(joined);
    StringBuilder out = new StringBuilder();
    int last = 0;
    while (m.find()) {
      String decoded = decodeWord(m.group(1), m.group(2), m.group(3));
      out.append(joined, last, m.start()).append(decoded != null ? decoded : m.group());
      last = m.end();
    }
    if (last == 0) {
      return s; // nothing decoded: keep the original spacing
    }
    return out.append(joined, last, joined.length()).toString();
  }

  private static String decodeWord(String charset, String encoding, String text) {
    int star = charset.indexOf('*'); // RFC 2231 language suffix
    String cs = (star >= 0 ? charset.substring(0, star) : charset).toLowerCase(Locale.ROOT);
    if (!cs.equals("utf-8") && !cs.equals("utf8") && !cs.equals("us-ascii")) {
      return null;
    }
    byte[] bytes;
    if (encoding.equalsIgnoreCase("B")) {
      try {
        bytes = Base64.getDecoder().decode(text);
      } catch (IllegalArgumentException e) {
        return null;
      }
    } else {
      bytes = decodeQ(text);
      if (bytes == null) {
        return null;
      }
    }
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private static byte[] decodeQ(String text) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c == '_') {
        out.write(' ');
      } else if (c == '=') {
        if (i + 2 >= text.length()) {
          return null;
        }
        int hi = Character.digit(text.charAt(i + 1), 16);
        int lo = Character.digit(text.charAt(i + 2), 16);
        if (hi < 0 || lo < 0) {
          return null;
        }
        out.write(hi * 16 + lo);
        i += 2;
      } else if (c < 128) {
        out.write(c);
      } else {
        return null;
      }
    }
    return out.toByteArray();
  }
}
