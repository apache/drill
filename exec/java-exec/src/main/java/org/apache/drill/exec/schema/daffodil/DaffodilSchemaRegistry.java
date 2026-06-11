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

import java.util.Iterator;
import java.util.Map;

/**
 * Persistent Registry for Daffodil Schemata
 */
public interface DaffodilSchemaRegistry extends AutoCloseable {

  /**
   * Creates a token table for specified {@code pluginName}.
   * @param schemaName The name of the plugin instance.
   */
  void createSchemaTable(String schemaName);

  PersistentSchemaTable getSchemaTable(String name);

  /**
   * Deletes the schema table for specified {@code pluginName}.
   * @param pluginName name of the plugin whose token table should be removed
   */
  void deleteSchemaTable(String pluginName);

  /**
   * Returns iterator for aliases table entries.
   */
  Iterator<Map.Entry<String, TupleMetadata>> getAllSchemata();
}
