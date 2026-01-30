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

package org.apache.drill.exec.schema.daffodil;

import org.apache.drill.exec.record.metadata.TupleMetadata;

public interface DaffodilSchemata {
  /**
   * Key of {@link this} Daffodil schema.
   */
  String getKey();

  /**
   * Gets the current Drill representation of a Daffodil schema
   *
   * @return The current access token
   */
  TupleMetadata getSchema();

  /**
   * Sets the schema
   *
   * @param drillSchema The incoming schema.
   */
  void setSchema(TupleMetadata drillSchema);

  /**
   * Returns value from schemata table that corresponds to provided plugin.
   *
   * @param schemaName name of desired Daffodil schema
   * @return The Desired Daffodil schema or {@code null} if no such schema exists.
   */
  TupleMetadata get(String schemaName);

  /**
   * Associates provided token with provided plugin in token table.
   *
   * @param token   Token of the value to associate with
   * @param value   Value that will be associated with provided alias
   * @param replace Whether existing value for the same token should be replaced
   * @return {@code true} if provided token was associated with
   * the provided value in tokens table
   */
  boolean put(String token, String value, boolean replace);

  /**
   * Removes value for specified token from tokens table.
   * @param token token of the value to remove
   * @return {@code true} if the value associated with
   * provided token was removed from the tokens table.
   */
  boolean remove(String schema);
}
