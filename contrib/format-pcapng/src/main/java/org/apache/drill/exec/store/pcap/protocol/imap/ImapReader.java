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
package org.apache.drill.exec.store.pcap.protocol.imap;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.mail.MailLines;

/**
 * Reads IMAP lines and values (atoms, quoted strings, {n} literals and parenthesized lists) from one
 * direction of a session. Literal lengths are checked against the bytes available.
 */
final class ImapReader {
  static final int MAX_DEPTH = 16;
  private static final int MAX_LITERAL_DIGITS = 10;

  /** An unquoted value such as NIL, a number, a flag or BODY[HEADER]. */
  static final class Atom {
    final String text;

    Atom(String text) {
      this.text = text;
    }
  }

  /** A {n} literal: n bytes of the stream. */
  static final class Literal {
    final byte[] data;
    final int start;
    final int length;

    Literal(byte[] data, int start, int length) {
      this.data = data;
      this.start = start;
      this.length = length;
    }
  }

  /** Why reading stopped. */
  static final class Problem extends RuntimeException {
    enum Kind { SYNTAX, INCOMPLETE, LITERAL }

    final Kind kind;
    final long literalLength;

    Problem(Kind kind, long literalLength) {
      super(kind.name(), null, false, false);
      this.kind = kind;
      this.literalLength = literalLength;
    }
  }

  private final byte[] d;
  private int pos;

  ImapReader(byte[] data) {
    this.d = data;
  }

  byte[] data() {
    return d;
  }

  int position() {
    return pos;
  }

  boolean atEnd() {
    return pos >= d.length;
  }

  /** Index of the LF ending the current line, or -1. */
  int lineEnd() {
    return MailLines.lineEnd(d, pos, d.length);
  }

  private static Problem syntax() {
    return new Problem(Problem.Kind.SYNTAX, -1);
  }

  private static Problem incomplete() {
    return new Problem(Problem.Kind.INCOMPLETE, -1);
  }

  private boolean lineBreak(int i) {
    return d[i] == '\r' || d[i] == '\n';
  }

  /** Reads a token up to a space or the line end and consumes one following space; null at the line end. */
  String token() {
    if (pos >= d.length) {
      throw incomplete();
    }
    int start = pos;
    while (pos < d.length && d[pos] != ' ' && !lineBreak(pos)) {
      pos++;
    }
    if (pos == d.length) {
      throw incomplete();
    }
    if (pos == start) {
      return null;
    }
    String token = MailLines.cap(new String(d, start, pos - start, StandardCharsets.ISO_8859_1));
    if (d[pos] == ' ') {
      pos++;
    }
    return token;
  }

  /** Reads the rest of the line as text, showing literals as {n} without their bytes, and consumes the line end. */
  String restOfLine() {
    StringBuilder text = new StringBuilder();
    while (true) {
      int lf = lineEnd();
      if (lf < 0) {
        throw incomplete();
      }
      String part = MailLines.text(d, pos, lf);
      pos = lf + 1;
      if (text.length() < MailLines.MAX_STRING) {
        text.append(part);
      }
      long n = trailingLiteral(part);
      if (n < 0) {
        return MailLines.cap(text.toString());
      }
      if (n > d.length - pos) {
        throw new Problem(Problem.Kind.LITERAL, n);
      }
      pos += (int) n;
    }
  }

  /** The length of a {n} or {n+} literal ending the line, or -1. */
  private static long trailingLiteral(String line) {
    if (!line.endsWith("}")) {
      return -1;
    }
    int open = line.lastIndexOf('{');
    if (open < 0) {
      return -1;
    }
    String digits = line.substring(open + 1, line.length() - 1);
    if (digits.endsWith("+")) {
      digits = digits.substring(0, digits.length() - 1);
    }
    if (digits.isEmpty()) {
      return -1;
    }
    for (int i = 0; i < digits.length(); i++) {
      if (digits.charAt(i) < '0' || digits.charAt(i) > '9') {
        return -1;
      }
    }
    if (digits.length() > MAX_LITERAL_DIGITS) {
      throw syntax();
    }
    return Long.parseLong(digits);
  }

  /** Reads values up to the end of the line and consumes the line end. */
  List<Object> values() {
    List<Object> values = new ArrayList<>();
    while (true) {
      skipSpaces();
      if (d[pos] == '\r' || d[pos] == '\n') {
        endLine();
        return values;
      }
      values.add(value(0));
    }
  }

  private void skipSpaces() {
    while (pos < d.length && d[pos] == ' ') {
      pos++;
    }
    if (pos >= d.length) {
      throw incomplete();
    }
  }

  private void endLine() {
    if (d[pos] == '\r') {
      pos++;
      if (pos >= d.length) {
        throw incomplete();
      }
      if (d[pos] != '\n') {
        throw syntax();
      }
    }
    pos++;
  }

  private Object value(int depth) {
    if (depth > MAX_DEPTH) {
      throw syntax();
    }
    byte c = d[pos];
    if (c == '(') {
      pos++;
      List<Object> list = new ArrayList<>();
      while (true) {
        skipSpaces();
        if (d[pos] == ')') {
          pos++;
          return list;
        }
        if (lineBreak(pos)) {
          throw syntax();
        }
        list.add(value(depth + 1));
      }
    }
    if (c == '"') {
      return quoted();
    }
    if (c == '{') {
      return literal();
    }
    return atom();
  }

  private String quoted() {
    pos++;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    while (true) {
      if (pos >= d.length) {
        throw incomplete();
      }
      byte b = d[pos++];
      if (b == '"') {
        break;
      }
      if (b == '\r' || b == '\n') {
        throw syntax();
      }
      if (b == '\\') {
        if (pos >= d.length) {
          throw incomplete();
        }
        b = d[pos++];
      }
      if (out.size() < MailLines.MAX_STRING * 4) {
        out.write(b);
      }
    }
    return MailLines.cap(new String(out.toByteArray(), StandardCharsets.UTF_8));
  }

  private Literal literal() {
    pos++;
    long n = 0;
    int digits = 0;
    while (pos < d.length && d[pos] >= '0' && d[pos] <= '9') {
      if (++digits > MAX_LITERAL_DIGITS) {
        throw syntax();
      }
      n = n * 10 + (d[pos++] - '0');
    }
    if (pos < d.length && d[pos] == '+') {
      pos++;
    }
    if (pos >= d.length) {
      throw incomplete();
    }
    if (digits == 0 || d[pos] != '}') {
      throw syntax();
    }
    pos++;
    if (pos >= d.length) {
      throw incomplete();
    }
    if (!lineBreak(pos)) {
      throw syntax();
    }
    endLine();
    if (n > d.length - pos) {
      throw new Problem(Problem.Kind.LITERAL, n);
    }
    Literal literal = new Literal(d, pos, (int) n);
    pos += (int) n;
    return literal;
  }

  private Atom atom() {
    int start = pos;
    while (pos < d.length) {
      byte b = d[pos];
      if (b == ' ' || b == '(' || b == ')' || b == '"' || b == '{' || lineBreak(pos)) {
        break;
      }
      if (b == '[') {
        // A section such as BODY[HEADER.FIELDS (FROM TO)] is part of the atom
        while (pos < d.length && d[pos] != ']') {
          if (lineBreak(pos)) {
            throw syntax();
          }
          pos++;
        }
        if (pos >= d.length) {
          throw incomplete();
        }
      }
      pos++;
    }
    if (pos >= d.length) {
      throw incomplete();
    }
    if (pos == start) {
      throw syntax();
    }
    return new Atom(MailLines.cap(new String(d, start, pos - start, StandardCharsets.ISO_8859_1)));
  }

  /** The text of a string value: an atom (NIL is null), a quoted string or a literal. */
  static String string(Object value) {
    if (value instanceof Atom) {
      String text = ((Atom) value).text;
      return text.equalsIgnoreCase("NIL") ? null : text;
    }
    if (value instanceof String) {
      return (String) value;
    }
    if (value instanceof Literal) {
      Literal l = (Literal) value;
      return MailLines.cap(new String(l.data, l.start, Math.min(l.length, MailLines.MAX_STRING * 4),
          StandardCharsets.UTF_8));
    }
    return null;
  }

  /** Values as text: literals are shown as {n} without their bytes. */
  static String render(List<Object> values) {
    StringBuilder out = new StringBuilder();
    render(values, out);
    return MailLines.cap(out.toString());
  }

  @SuppressWarnings("unchecked")
  private static void render(List<Object> values, StringBuilder out) {
    for (int i = 0; i < values.size() && out.length() < MailLines.MAX_STRING; i++) {
      if (i > 0) {
        out.append(' ');
      }
      Object v = values.get(i);
      if (v instanceof Atom) {
        out.append(((Atom) v).text);
      } else if (v instanceof String) {
        out.append('"').append(((String) v).replace("\\", "\\\\").replace("\"", "\\\"")).append('"');
      } else if (v instanceof Literal) {
        out.append('{').append(((Literal) v).length).append('}');
      } else {
        out.append('(');
        render((List<Object>) v, out);
        out.append(')');
      }
    }
  }
}
