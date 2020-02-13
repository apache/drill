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

  /**
   * Called at the start of a set of values for an object. That is, called
   * when the structure parser accepts the <code>{</code> token.
   */
  void onStart();

  /**
   * Called at the end of a set of values for an object. That is, called
   * when the structure parser accepts the <code>}</code> token.
   */
  void onEnd();

  /**
   * Called by the structure parser when it first sees a new field for
   * and object to determine if that field is to be projected (is needed
   * by the listener.) If not projected, the structure parser will not
   * ask for a value listener and will insert a "dummy" parser that will
   * free-wheel over any value of that field. As a result, unprojected
   * fields can not cause type errors: they are invisible as long as
   * they are syntactically valid.
   *
   * @param key the object field name
   * @return {@code true} if this listener wants to provide a listener
   * for the field, {@code false} if the field should be ignored
   */
  boolean isProjected(String key);

  /**
   * A new field has appeared with a scalar (or {@code null}) value.
   * That is: {@code key: <scalar>}.
   *
   * @param key the field name
   * @param type the type as given by the JSON token for the value
   * @return a value listener for the scalar value
   */
  ValueListener addScalar(String key, JsonType type);

  /**
   * A new field has appeared with a scalar, {@code null} or empty array
   * value. That is, one of:
   * <ul>
   * <li><code>key: [+ &lt;scalar></code></li>
   * <li><code>key: [+ null</code></li>
   * <li><code>key: [+ ]</code></li>
   * </ul>
   * Where "[+" means one or more opening array elements.
   *
   * @param key the field name
   * @param arrayDims number of dimensions observed in the first appearance
   * of the array (more may appear later)
   * @param type the observed type of the first element of the array, or
   * {@link JsonType.NULL} if {@code null} was see, or
   * {@link JsonType.EMPTY} if an empty array was seen
   * @return a listener for the field itself which is prepared to
   * return an array listener
   */
  ValueListener addArray(String key, int arrayDims, JsonType type);

  /**
   * A new field has appeared with an object value.
   * That is: {@code key: <scalar>}.
   *
   * @param key the field name
   * @return a value listener which assumes the value is an object
   */
  ValueListener addObject(String key);

  /**
   * A new field has appeared with an object array value.
   * That is: <code>key: ]+ {</code>.
   *
   * @param key the field name
   * @return a value listener which assumes the value is an object
   * array
   */
  ValueListener addObjectArray(String key, int dims);
}
