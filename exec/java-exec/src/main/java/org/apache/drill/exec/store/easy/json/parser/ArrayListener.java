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
 * Represents one level within an array. The first time the parser sees
 * the array element, it will call one of the "Element" methods with the
 * look-ahead values visible to the parser. Since JSON is flexible, later
 * data shapes may not necessarily follow the first shape. The implementation
 * must handle this or throw an error if not supported.
 * <p>
 * When creating a multi-dimensional array, each array level is built one
 * by one. each will receive the same type information (decreased by one
 * array level.)
 * <p>
 * Then, while parsing, the parser calls events on the start and end of the
 * array, as well as on each element.
 * <p>
 * The array listener is an attribute of a value listener, represent the
 * "arrayness" of that value, if the value allows an array.
 */
public interface ArrayListener {

  void onStart();
  void onElement();
  void onEnd();

  ValueListener objectArrayElement(int arrayDims);
  ValueListener objectElement();
  ValueListener arrayElement(int arrayDims, JsonType type);
  ValueListener scalarElement(JsonType type);
}
