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
package org.apache.drill.exec.server.rest.workflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Recursive-descent parser for workflow condition expressions.
 * <pre>
 * expr    := or
 * or      := and ( "||" and )*
 * and     := unary ( "&&" unary )*
 * unary   := "!" unary | cmp
 * cmp     := "(" expr ")" | path ( OP literal )?
 * path    := ident ( "." ident | "[*]" )*
 * literal := number | 'string' | "string" | true | false | null
 * </pre>
 * The left side of a comparison is always a field path, which keeps the tree shape
 * identical to what the structured builder produces.
 */
public final class ConditionParser {

  public static class ParseException extends Exception {
    public final int position;

    public ParseException(String message, int position) {
      super(message + " at position " + position);
      this.position = position;
    }
  }

  private static final JsonNodeFactory F = JsonNodeFactory.instance;
  private static final String[] SYMBOL_OPS = {"==", "!=", ">=", "<=", ">", "<"};
  private static final String[] WORD_OPS = {"contains", "startsWith"};
  static final int MAX_DEPTH = 50;

  private final String src;
  private int pos;
  private int depth;

  private ConditionParser(String src) {
    this.src = src;
  }

  public static JsonNode parse(String expr) throws ParseException {
    ConditionParser p = new ConditionParser(expr == null ? "" : expr);
    p.skipWs();
    if (p.atEnd()) {
      throw new ParseException("Empty expression", 0);
    }
    JsonNode node = p.or();
    p.skipWs();
    if (!p.atEnd()) {
      throw new ParseException("Unexpected '" + p.src.charAt(p.pos) + "'", p.pos);
    }
    return node;
  }

  private JsonNode or() throws ParseException {
    List<JsonNode> parts = new ArrayList<>();
    parts.add(and());
    while (eat("||")) {
      parts.add(and());
    }
    return group("any", parts);
  }

  private JsonNode and() throws ParseException {
    List<JsonNode> parts = new ArrayList<>();
    parts.add(unary());
    while (eat("&&")) {
      parts.add(unary());
    }
    return group("all", parts);
  }

  private JsonNode unary() throws ParseException {
    skipWs();
    if (peekChar('!') && !src.startsWith("!=", pos)) {
      pos++;
      enter();
      ObjectNode not = F.objectNode();
      not.set("not", unary());
      depth--;
      return not;
    }
    return cmp();
  }

  private JsonNode cmp() throws ParseException {
    skipWs();
    if (eat("(")) {
      enter();
      JsonNode inner = or();
      if (!eat(")")) {
        throw new ParseException("Expected ')'", pos);
      }
      depth--;
      return inner;
    }
    int start = pos;
    String path = path();
    if (path == null) {
      throw new ParseException("Expected a field name", start);
    }
    skipWs();
    String op = op();
    if (op == null) {
      return leaf(path, "==", F.booleanNode(true));
    }
    skipWs();
    return leaf(path, op, literal());
  }

  private void enter() throws ParseException {
    if (++depth > MAX_DEPTH) {
      throw new ParseException("Expression nested too deeply", pos);
    }
  }

  private String path() {
    int start = pos;
    if (!identStart()) {
      return null;
    }
    ident();
    while (true) {
      if (src.startsWith("[*]", pos)) {
        pos += 3;
      } else if (peekChar('.') && pos + 1 < src.length() && isIdentStart(src.charAt(pos + 1))) {
        pos++;
        ident();
      } else {
        break;
      }
    }
    String path = src.substring(start, pos);
    if (path.equals("true") || path.equals("false") || path.equals("null")) {
      pos = start;
      return null;
    }
    return path;
  }

  private String op() {
    for (String op : SYMBOL_OPS) {
      if (src.startsWith(op, pos)) {
        pos += op.length();
        return op;
      }
    }
    for (String op : WORD_OPS) {
      int end = pos + op.length();
      if (src.startsWith(op, pos) && (end >= src.length() || !isIdentPart(src.charAt(end)))) {
        pos = end;
        return op;
      }
    }
    return null;
  }

  private JsonNode literal() throws ParseException {
    int start = pos;
    if (atEnd()) {
      throw new ParseException("Expected a value", pos);
    }
    char c = src.charAt(pos);
    if (c == '\'' || c == '"') {
      return F.textNode(string(c));
    }
    if (c == '-' || Character.isDigit(c)) {
      pos++;
      while (!atEnd() && (Character.isDigit(src.charAt(pos)) || src.charAt(pos) == '.')) {
        pos++;
      }
      String num = src.substring(start, pos);
      try {
        if (num.contains(".")) {
          return F.numberNode(Double.parseDouble(num));
        }
        long v = Long.parseLong(num);
        return v == (int) v ? F.numberNode((int) v) : F.numberNode(v);
      } catch (NumberFormatException e) {
        throw new ParseException("Invalid number '" + num + "'", start);
      }
    }
    for (String kw : new String[] {"true", "false", "null"}) {
      int end = pos + kw.length();
      if (src.startsWith(kw, pos) && (end >= src.length() || !isIdentPart(src.charAt(end)))) {
        pos = end;
        return kw.equals("null") ? F.nullNode() : F.booleanNode(kw.equals("true"));
      }
    }
    throw new ParseException("Expected a value", start);
  }

  private String string(char quote) throws ParseException {
    int start = pos;
    pos++;
    StringBuilder sb = new StringBuilder();
    while (!atEnd()) {
      char c = src.charAt(pos++);
      if (c == quote) {
        return sb.toString();
      }
      if (c == '\\' && !atEnd()) {
        c = src.charAt(pos++);
      }
      sb.append(c);
    }
    throw new ParseException("Unterminated string", start);
  }

  private static JsonNode group(String key, List<JsonNode> parts) {
    if (parts.size() == 1) {
      return parts.get(0);
    }
    ObjectNode node = F.objectNode();
    ArrayNode arr = node.putArray(key);
    parts.forEach(arr::add);
    return node;
  }

  private static JsonNode leaf(String field, String op, JsonNode value) {
    ObjectNode n = F.objectNode();
    n.put("field", field);
    n.put("op", op);
    n.set("value", value);
    return n;
  }

  private boolean eat(String token) {
    skipWs();
    if (src.startsWith(token, pos)) {
      pos += token.length();
      return true;
    }
    return false;
  }

  private void ident() {
    while (!atEnd() && isIdentPart(src.charAt(pos))) {
      pos++;
    }
  }

  private boolean identStart() {
    return !atEnd() && isIdentStart(src.charAt(pos));
  }

  private static boolean isIdentStart(char c) {
    return Character.isLetter(c) || c == '_';
  }

  private static boolean isIdentPart(char c) {
    return Character.isLetterOrDigit(c) || c == '_';
  }

  private boolean peekChar(char c) {
    return !atEnd() && src.charAt(pos) == c;
  }

  private void skipWs() {
    while (!atEnd() && Character.isWhitespace(src.charAt(pos))) {
      pos++;
    }
  }

  private boolean atEnd() {
    return pos >= src.length();
  }
}
