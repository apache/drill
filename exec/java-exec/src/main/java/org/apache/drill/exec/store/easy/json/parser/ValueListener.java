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
 * <li>Etc.</li>
 * </ul>
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
 */
public interface ValueListener {

  boolean isText();
  void onNull();
  void onBoolean(boolean value);
  void onInt(long value);
  void onFloat(double value);
  void onString(String value);
  void onEmbedddObject(String value);
  ObjectListener object();
  ArrayListener array(int arrayDims, JsonType type);
  ArrayListener objectArray(int arrayDims);
}
