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
 */
public interface ObjectListener {

  void onStart();

  void onEnd();

  boolean isProjected(String key);

  ValueListener addScalar(String key, JsonType type);

  /**
   * Add an array field which is either scalar or of unknown type.
   *
   * @param key field name
   * @param dims number of dimensions observed in the first appearance
   * of the array (more may appear later)
   * @param type the observed type of the array, or
   * {@link JsonType.NULL} if {@code null} was see, or
   * {@link JsonType.EMPTY} if an empty array was seen
   * @return a listener for the field itself which is prepared to
   * return an array listener
   */
  ValueListener addArray(String key, int dims, JsonType type);
  ValueListener addObject(String key);
  ValueListener addObjectArray(String key, int dims);
}
