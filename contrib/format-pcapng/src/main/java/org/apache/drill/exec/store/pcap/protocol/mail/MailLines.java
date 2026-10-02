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

import java.nio.charset.StandardCharsets;

/** Reads LF or CRLF terminated lines from one direction of a mail protocol session. */
public final class MailLines {
  public static final int MAX_STRING = 4096;
  public static final int MAX_ITEMS = 64;
  /** Session decoders stop after this many client commands. */
  public static final int MAX_COMMANDS = 1000;

  private final byte[] data;
  private int pos;

  public MailLines(byte[] data) {
    this.data = data;
  }

  public byte[] data() {
    return data;
  }

  public int position() {
    return pos;
  }

  public void seek(int position) {
    pos = position;
  }

  public boolean atEnd() {
    return pos >= data.length;
  }

  /** Index of the LF ending the current line, or -1 if the line is incomplete. */
  public int lineEnd() {
    return lineEnd(data, pos, data.length);
  }

  /** The next complete line without its terminator, or null (position unchanged) if there is none. */
  public String next() {
    int lf = lineEnd();
    if (lf < 0) {
      return null;
    }
    String line = text(data, pos, lf);
    pos = lf + 1;
    return line;
  }

  public static int lineEnd(byte[] d, int from, int end) {
    for (int i = from; i < end; i++) {
      if (d[i] == '\n') {
        return i;
      }
    }
    return -1;
  }

  /** The text of d[from, lf) without a trailing CR, capped at MAX_STRING characters. */
  public static String text(byte[] d, int from, int lf) {
    return cap(text(d, from, lf, MAX_STRING));
  }

  /** The text of d[from, lf) without a trailing CR, capped at maxChars characters. */
  public static String text(byte[] d, int from, int lf, int maxChars) {
    int end = lf;
    if (end > from && d[end - 1] == '\r') {
      end--;
    }
    int length = Math.min(end - from, maxChars * 4);
    String s = new String(d, from, length, StandardCharsets.UTF_8);
    return s.length() <= maxChars ? s : s.substring(0, maxChars);
  }

  public static String cap(String s) {
    return s == null || s.length() <= MAX_STRING ? s : s.substring(0, MAX_STRING);
  }

  /** The first complete line of the data, or null. */
  public static String firstLine(byte[] d) {
    int lf = lineEnd(d, 0, d.length);
    return lf < 0 ? null : text(d, 0, lf);
  }
}
