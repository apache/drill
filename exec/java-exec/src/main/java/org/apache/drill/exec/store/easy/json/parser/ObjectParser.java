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
package org.apache.drill.exec.store.easy.json.parser;

import java.util.Map;

import org.apache.drill.common.map.CaseInsensitiveMap;

import com.fasterxml.jackson.core.JsonToken;

/**
 * Parses a JSON object: <code>{ name : value ... }</code>
 * <p>
 * Creates a map of known fields. Each time a field is parsed,
 * looks up the field in the map. If not found, the value is "sniffed"
 * to determine its type, and a matching parser and listener created.
 * Thereafter, the previous parser is reused.
 * <p>
 * The object listener provides semantics. One key decision is whether
 * to project a field or not. An unprojected field is parsed with
 * a "dummy" parser that "free-wheels" over all valid JSON structures.
 * Otherwise, the listener is given whatever type information that the
 * parser can discover when creating the field.
 * <p>
 * Work is divided between this class, which discovers fields, and
 * the listeners which determine the meaning of field values. A field,
 * via a properly-defined listener, can accept one or more different
 * value kinds.
 * <p>
 * The parser accepts JSON tokens as they appear in the file. The
 * question of whether those tokens make sense is left to the listeners.
 * The listeners decide if the tokens make sense for a particular column.
 * The listener should provide a clear error if a particular token is not
 * valid for a given listener.
 *
 * <h4>Nulls</h4>
 *
 * Null values are handled at the semantic, not syntax level. If the
 * first appearance of a field contains a null value, then the parser can
 * provide no hints about the expected field type. The listener must
 * implement a solution such as referring to a schema, waiting for a
 * non-null value to appear, etc.
 * <p>
 * Since the parser classes handle syntax, they are blissfully ignorant
 * of any fancy logic needed for null handling. Each field is
 * represented by a field parser whether that field is null or not.
 * It is the listener that may have to swap out one mechanism for
 * another as types are discovered.
 *
 * <h4>Complex Types</h4>
 *
 * Parsers handle arrays and objects using a two-level system. Each field
 * always is driven by a field parser. If the field is discovered to be an
 * array, then we add an array parser to the field parser to handle array
 * contents. The same is true of objects.
 * <p>
 * Both objects and arrays are collections of values, and a value can
 * optionally contain an array or object. (JSON allows any given field
 * name to map to both objects and arrays in different rows. The parser
 * structure reflects this syntax. The listeners can enforce more
 * relational-like semantics).
 * <p>
 * If an array is single-dimension, then the field parse contains an array
 * parser which contains another value parser for the array contents. If
 * the array is multi-dimensional, there will be multiple array/value
 * parser pairs: one for each dimension.
 */
public class ObjectParser extends AbstractElementParser {
  private final ObjectListener listener;
  private final Map<String, ElementParser> members = CaseInsensitiveMap.newHashMap();

  public ObjectParser(ElementParser parent, ObjectListener listener) {
    super(parent);
    this.listener = listener;
  }

  public ObjectListener listener() { return listener; }

  /**
   * Parses <code>{ ^ ... }</code>
   */
  @Override
  public void parse(TokenIterator tokenizer) {
    listener.onStart();

    // Parse (field: value)* }

    top: for (;;) {
      JsonToken token = tokenizer.requireNext();
      // Position: { (key: value)* ? ^
      switch (token) {
        case END_OBJECT:
          // Position: { (key: value)* } ^
          break top;

        case FIELD_NAME:
          // Position: { (key: value)* key: ^
          parseMember(tokenizer);
          break;

        default:
          // Position: { (key: value)* ~(key | }) ^
          // Invalid JSON.
          // Actually, we probably won't get here, the JSON parser
          // itself will throw an exception.
          throw errorFactory().syntaxError(token);
      }
    }
    listener.onEnd();
  }

  /**
   * Parse a field. Two cases. First, this is a field we've already seen. If so,
   * look up the parser for that field and use it. If this is the first time
   * we've seen the field, "sniff" tokens to determine field type, create a
   * parser, then parse.
   */
  private void parseMember(TokenIterator tokenizer) {
    // Position: key: ^ ?
    final String key = tokenizer.textValue().trim();
    ElementParser fieldParser = members.get(key);
    if (fieldParser == null) {
      // New key; sniff the value to determine the parser to use
      // (which also tell us the kind of column to create in Drill.)
      // Position: key: ^
      fieldParser = detectValueParser(tokenizer, key);
      members.put(key, fieldParser);
    }
    // Parse the field value.
    // Position: key: ^ value ...
    fieldParser.parse(tokenizer);
  }

  /**
   * If the column is not projected, create a dummy parser to "free wheel" over
   * the value. Otherwise, look ahead a token or two to determine the the type
   * of the field. Then the caller will backtrack to parse the field.
   *
   * @param key name of the field
   * @return parser for the field
   */
  private ElementParser detectValueParser(TokenIterator tokenizer, final String key) {
    if (key.isEmpty()) {
      throw errorFactory().structureError(
          "Drill does not allow empty keys in JSON key/value pairs");
    }
    if (!listener.isProjected(key)) {
      return new DummyValueParser(this);
    }
    return ValueFactory.createFieldParser(this, key, tokenizer);
  }
}
