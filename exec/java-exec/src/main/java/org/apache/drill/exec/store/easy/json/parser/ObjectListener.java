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

import org.apache.calcite.model.JsonType;

/**
 * Represents events on a object value. The object value may be a top-level
 * field or may be the element of an array. The listener gets an event when
 * an object is started and ended, as well as when a new field is discovered.
 * First, the parser asks if the field should be projected. If not, the
 * parser will create a dummy parser to "free-wheel" over whatever values the
 * field contains. (This is one way to avoid structure errors in a JSON file:
 * just ignore them.) Otherwise, the parser will look ahead to guess the
 * field type and will call one of the "add" methods, each of which should
 * return a value listener for the field itself.
 * <p>
 * The structure parser looks ahead some number of tokens to infer the value
 * of the field. While this is helpful, it really only works if the JSON
 * is structured like a list of tuples, if the initial value is not {@code null},
 * and if initial arrays are not empty. The structure parser cannot see
 * into the future beyond the first field value; the value listener for each
 * field must handle "type-deferal" if needed to handle missing or null
 * values. That is, type-consistency is a semantic task handled by the listener,
 * not a syntax task handled by the parser.
 *
 * <h4>Fields</h4>
 *
 * The structure of an object is:
 * <ul>
 * <li>{@code ObjectListener} which represents the object (tuple) as a whole.
 * Each field, indexed by name, is represented as a</li>
 * <li>{@code ValueListener} which represents the value "slot". That value
 * can be scalar, or can be structured, in which case the value listener
 * contains either a</li>
 * <li>{@code ArrayListener} for an array, or a</li>
 * <li>{@code ObjectListener} for a nested object (tuple).</li>
 * </ul>
 */
public interface ObjectListener {

  enum FieldType {

    /**
     * The field is unprojected, ignore its content. No value listener
     * is created.
     */
    IGNORE,

    /**
     * Parse the JSON object according to its type.
     */
    TYPED,

    /**
     * The field is to be treated as "all-text". Used when the parser-level
     * setting for {@code allTextMode} is {@code false}; allows per-field
     * overrides to, perhaps, ride over inconsistent scalar types for a
     * single field. The listener will receive only strings.
     */
    TEXT,

    /**
     * Parse the value, and all its children, as JSON.
     * That is, converts the parsed JSON back into a
     * JSON string. The listener will receive only strings.
     */
    JSON
  }

  /**
   * Called at the start of a set of values for an object. That is, called
   * when the structure parser accepts the <code>{</code> token.
   */
  void onStart();

  /**
   * Called by the structure parser when it first sees a new field for
   * and object to determine how to parse the field.
   * If not projected, the structure parser will not
   * ask for a value listener and will insert a "dummy" parser that will
   * free-wheel over any value of that field. As a result, unprojected
   * fields can not cause type errors: they are invisible as long as
   * they are syntactically valid.
   * <p>
   * The {@link FieldType#JSON} type says to parse the entire field, and
   * its children, as a JSON string. The parser will ask for a value
   * listener to accept the JSON text.
   *
   * @param key the object field name
   * @return how the field should be parsed
   */
  FieldType fieldType(String key);

  /**
   * The structure parser has just encountered a new field for this
   * object. The {@link #fieldType(String)} indicated that the field is
   * to be projected. This method performs any setup needed to handle the
   * field, then returns a value listener to receive events for the
   * field value. The value listener may be asked to create additional
   * structure, such as arrays or nested objects.
   *
   * @param key the field name
   * @param valueDef a description of the field as inferred by looking
   * ahead some number of tokens in the input JSON. Provides both a data
   * type and array depth (dimensions.) If the type is
   * {@link JsonType#NONE EMPTY}, then the field is an empty array.
   * If the type is {@link JsonType#NULL NULL}, then the value is null. In these
   * cases, the listener can replace itself when an actual value appears
   * later
   * @return a listener to receive events for the newly-created field
   */
  ValueListener addField(String key, ValueDef valueDef);

  /**
   * Called at the end of a set of values for an object. That is, called
   * when the structure parser accepts the <code>}</code> token.
   */
  void onEnd();
}
