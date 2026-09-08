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
package org.apache.drill.exec.store.accumulo.schema;

import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;

/**
 * Interface for schema discovery strategies in the Accumulo storage plugin.
 *
 * <p>This interface abstracts how table schemas are discovered from Accumulo.
 * Different implementations can provide schema information from different sources:</p>
 * <ul>
 *   <li>{@code MetadataTableSchemaProvider} - Reads schema from a dedicated Accumulo metadata table</li>
 *   <li>{@code ScanSamplingSchemaProvider} - Infers schema by sampling table data (future)</li>
 *   <li>{@code ConfigFileSchemaProvider} - Reads schema from external configuration files (future)</li>
 * </ul>
 *
 * <p>This is the extension point for Option B (advanced mode) where custom schema
 * providers could expose Accumulo-specific features like iterators.</p>
 */
public interface AccumuloSchemaProvider {

  /**
   * Returns the schema for the specified Accumulo table.
   *
   * <p>If the schema is not found or cannot be determined, implementations should
   * return a dynamic schema (via {@link TableSchema#dynamic(String)}) rather than
   * throwing an exception.</p>
   *
   * @param client the Accumulo client
   * @param tableName the name of the table
   * @return the table schema, never null
   */
  TableSchema getTableSchema(AccumuloClient client, String tableName);

  /**
   * Discovers all table names available in the Accumulo instance.
   *
   * <p>Implementations should filter out system tables (e.g., tables starting with "accumulo.")
   * unless specifically configured to include them.</p>
   *
   * @param client the Accumulo client
   * @return set of table names, never null (may be empty)
   */
  Set<String> discoverTableNames(AccumuloClient client);

  /**
   * Returns true if this provider has schema information for the specified table.
   *
   * <p>This can be used to check if explicit schema metadata exists before
   * falling back to dynamic schema discovery.</p>
   *
   * @param client the Accumulo client
   * @param tableName the name of the table
   * @return true if schema information is available
   */
  boolean hasSchema(AccumuloClient client, String tableName);

  /**
   * Clears any cached schema information.
   *
   * <p>Called when schema metadata may have changed and needs to be refreshed.</p>
   */
  void clearCache();

  /**
   * Clears cached schema information for a specific table.
   *
   * @param tableName the table to clear from cache
   */
  void clearCache(String tableName);
}
