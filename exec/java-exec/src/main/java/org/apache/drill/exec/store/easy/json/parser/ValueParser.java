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

import org.apache.drill.exec.store.easy.json.parser.ObjectListener.FieldType;
import org.apache.drill.exec.store.easy.json.parser.ValueListener.ValueHost;

import com.fasterxml.jackson.core.JsonToken;

/**
 * Parses a JSON value. JSON allows any value type to appear anywhere a
 * value is allowed; this parser reflects that rule. The associated listener
 * is responsible for semantics: whether a given value should be allowed.
 * <p>
 * Scalar value processing occurs in one of two ways:
 * <ul>
 * <li><b>Typed</b>: The type of the JSON value determines which of the
 * listener "on" method is called. This ensures that the JSON text
 * is parsed into typed values using JSON's parsing rules.</li>
 * <li><b>Text</b>: The text value is passed to the listener's
 * {@code onString()} method regardless of the JSON type. (That is,
 * according to Drill's "all-text mode."</li>
 * </ul>
 * Listeners can enforce one type only, or can be more flexible and
 * allow multiple types.
 */
public class ValueParser extends AbstractElementParser implements ValueHost {

  private interface ValueHandler {
    void accept(TokenIterator tokenizer, JsonToken token);
  }

  /**
   * Parses <code>true | false | null | integer | float | string |<br>
   *              embedded-object</code>
   * <p>
   * Forwards the result as a typed value.
   */
  public class TypedValueHandler implements ValueHandler {

    @Override
    public void accept(TokenIterator tokenizer, JsonToken token) {
      switch (token) {
        case VALUE_TRUE:
          listener.onBoolean(true);
          break;
        case VALUE_FALSE:
          listener.onBoolean(false);
          break;
        case VALUE_NUMBER_INT:
          listener.onInt(tokenizer.longValue());
          break;
        case VALUE_NUMBER_FLOAT:
          listener.onFloat(tokenizer.doubleValue());
          break;
        case VALUE_STRING:
          listener.onString(tokenizer.stringValue());
          break;
        case VALUE_EMBEDDED_OBJECT:
          listener.onEmbeddedObject(tokenizer.stringValue());
        default:
          // Won't get here: the Jackson parser catches
          // errors.
          throw errorFactory().syntaxError(token);
      }
    }
  }

  /**
   * Parses <code>true | false | null | integer | float | string |<br>
   *              embedded-object</code>
   * <p>
   * Forwards the result as a string.
   */
  public class TextValueHandler implements ValueHandler {

    @Override
    public void accept(TokenIterator tokenizer, JsonToken token) {
      switch (token) {
        case VALUE_EMBEDDED_OBJECT:
        case VALUE_FALSE:
        case VALUE_TRUE:
        case VALUE_NUMBER_FLOAT:
        case VALUE_NUMBER_INT:
        case VALUE_STRING:
          listener.onString(tokenizer.textValue());
          break;

        default:
          // Won't get here: the Jackson parser catches
          // errors.
          throw errorFactory().syntaxError(token);
      }
    }
  }

  private final String key;
  protected final ValueHandler valueHandler;
  private ValueListener listener;
  private ObjectParser objectParser;
  private ArrayParser arrayParser;

  public ValueParser(ElementParser parent, String key, FieldType type) {
    super(parent);
    this.key = key;
    if (type == FieldType.TEXT || structParser().options().allTextMode) {
      valueHandler = new TextValueHandler();
    } else {
      valueHandler = new TypedValueHandler();
    }
  }

  @Override
  public void bindListener(ValueListener listener) {
    this.listener = listener;
    listener.bind(this);
    if (arrayParser != null) {
      arrayParser.bindListener(listener.array(ValueDef.UNKNOWN_ARRAY));
    }
  }

  public String key() { return key; }

  public ValueListener listener() { return listener; }

  /**
   * Parses <code>true | false | null | integer | float | string|
   *              embedded-object | { ... } | [ ... ]</code>
   */
  @Override
  public void parse(TokenIterator tokenizer) {
    JsonToken token = tokenizer.requireNext();
    switch (token) {
    case START_OBJECT:
      // Position: { ^
      if (objectParser == null) {
        // No object parser yet. May be that the value was null,
        // or may be that it changed types.
        addObjectParser();
      }
      objectParser.parse(tokenizer);
      break;

    case START_ARRAY:
      // Position: [ ^
      if (arrayParser == null) {
        // No array parser yet. May be that the value was null,
        // or may be that it changed types.
        addArrayParser(ValueDefFactory.arrayLookAhead(tokenizer));
      }
      arrayParser.parse(tokenizer);
      break;

    case VALUE_NULL:
      listener.onNull();
      break;

    default:
      valueHandler.accept(tokenizer, token);
    }
  }

  public void addObjectParser() {
    objectParser = new ObjectParser(this, listener().object());
  }

  private void addArrayParser(ValueDef valueDef) {
    ArrayListener arrayListener = listener().array(valueDef);
    arrayParser = new ArrayParser(this, arrayListener);
    arrayParser.expandStructure(valueDef);
  }

  public void expandStructure(ValueDef valueDef) {
    if (valueDef.isArray()) {
      addArrayParser(valueDef);
    } else if (valueDef.type().isObject()) {
      addObjectParser();
    }
  }
}
