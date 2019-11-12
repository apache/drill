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
package org.apache.drill.exec.vector.accessor;

import java.util.Map;

public interface DictReader extends ArrayReader {

  /**
   * Returns DICT's value {@link ObjectReader} with its position set to an entry associated with the key.
   * If the DICT does not contain the entry, {@link org.apache.drill.exec.vector.accessor.reader.NullReader}
   * is returned instead. To check if this is the case, use {@link ObjectReader#isNull()} on the returned value reader.
   *
   * @param key key identifying an entry
   * @return value reader with its position set accordingly or {@link org.apache.drill.exec.vector.accessor.reader.NullReader}
   * if there is no such entry
   */
  ObjectReader getValueReader(Object key);

  /**
   * Returns value that corresponds to the key (analogous to {@link Map#get(Object)}).
   * If there is no entry with specified key, {@code null} is returned.
   * @param key key associated with value
   * @return value associate with value
   */
  Object get(Object key);

  /**
   * Number of entries in the dict.
   * @return the number of entries
   */
  @Override
  int size();

  ValueType keyColumnType();

  ObjectType valueColumnType();
}
