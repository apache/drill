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

/**
 * Represents a JSON object, either a direct object field, or level
 * within an array. That is:
 * <ul>
 * <li>{@code foo: <value>} - Field value</li>
 * <li>{@code foo: [ <value> ]} - 1D array value</li>
 * <li>{@code foo: [ [<value> ] ]} - 2D array value</li>
 * <li><code>foo: { ... }</code> - object</li>
 * <li><code>foo: [+ { ... } ]</code> - object array</li>
 * </ul>
 * <p>
 * A value listener appears at each level of an array. The top
 * and inner dimensions will provide an array listener, the bottom
 * level (outermost dimension) will see the value events.
 * <p>
 * A field value can be a scalar, an array or an object.
 * The structured types return a child listener specialized for that
 * type. The parser asks for the structured listener only once, when
 * building the parse tree for that structure. The scalar value
 * methods are called each time a value is parsed. Note that, for
 * any given row, it may be that no method is called if the field
 * does not appear in that record.
 * <p>
 * Object and array listeners are given contextual information when
 * adding fields or elements. JSON allows any value to appear in any
 * context. So, as the parse proceeds, the
 * parser may come across a structure different than the initial hint.
 * For example, the initial value might be null, and the later value
 * might be an array. The initial value might be an integer, but the
 * later value could be an object. It
 * is up to the listener implementation to decide whether to support
 * such structures. The implementation should log a warning, or throw
 * an exception, if it does not support a particular event.
 * <p>
 * JSON is flexible. It could be that the first value seen for an element
 * is {@code null} (or a scalar) and so the parser calls a scalar
 * method on the value listener. Perhaps the next value is an object or
 * an array. The parser focuses only on syntax: the JSON is what it is.
 * The parser simply asks the listener for an array or object listener
 * (then caches the resulting listener). The value listener is responsible
 * for semantics: deciding if it is valid to mix types in a field.
 */
public interface ValueListener {

  /**
   * The field is to be treated as "all-text". Used when the parser-level
   * setting for {@code allTextMode} is {@code false}; allows per-field
   * overrides to, perhaps, ride over inconsistent scalar types for a
   * single field.
   *
   * @return {@code true} if the field is to be read in "all-text mode" even
   * if the global setting is off, {@code false} to read the field as
   * typed values.
   */
  boolean isText();

  /**
   * Called on parsing a {@code null} value for the field. Called whether
   * the field is parsed as all-text or as typed values.
   */
  void onNull();

  /**
   * Called for the JSON {@code true} or {@code false} values when parsing
   * the field as a typed field.
   *
   * @param value the Boolean value of the parsed token
   */
  void onBoolean(boolean value);

  /**
   * Called for JSON integer values when parsing the field as a typed
   * field.
   *
   * @param value the integer value of the parsed token
   */
  void onInt(long value);

  /**
   * Called for JSON float values when parsing the field as a typed
   * field.
   *
   * @param value the float value of the parsed token
   */
  void onFloat(double value);

  /**
   * Called for JSON string values when parsing the field as a typed
   * field, and for all non-null scalar values when parsed in
   * all-text mode
   *
   * @param value the string value of the parsed token
   */
  void onString(String value);

  /**
   * Called for embedded object values when parsing the field as a typed
   * field.
   * <p>
   * Note: This method is for completeness with the entire set of JSON
   * value tokens. It is not currently supported in Drill.
   *
   * @param value the string value of the parsed token
   */
  void onEmbedddObject(String value);

  /**
   * The parser has encountered a object value for the field for the first
   * time. That is: {@code foo: {</code}.
   *
   * @return an object listener for the object
   */
  ObjectListener object();

  /**
   * The parser has encountered a array value for the first
   * time, and that array is scalar, null or empty.
   *
   * @param arrayDims the number of observed array dimensions
   * @param type the observed JSON token type for the array element
   * @return an array listener for the array
   */
  ArrayListener array(int arrayDims, JsonType type);

  /**
   * The parser has encountered a array value for the first
   * time, and that array contains an object.
   *
   * @param arrayDims the number of observed array dimensions
   * @return an array listener for the array
   */
  ArrayListener objectArray(int arrayDims);
}
