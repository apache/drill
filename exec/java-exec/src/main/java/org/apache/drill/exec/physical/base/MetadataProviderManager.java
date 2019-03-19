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
package org.apache.drill.exec.physical.base;

import org.apache.drill.exec.planner.common.DrillStatsTable;
import org.apache.drill.exec.record.metadata.schema.SchemaProvider;

/**
 * Base interface for passing and obtaining {@link SchemaProvider}, {@link DrillStatsTable} and
 * {@link TableMetadataProvider}, responsible for creating required
 * {@link TableMetadataProviderBuilder} which constructs required {@link TableMetadataProvider}
 * based on specified providers
 */
public interface MetadataProviderManager {

  void setSchemaProvider(SchemaProvider schemaProvider);

  SchemaProvider getSchemaProvider();

  void setStatsProvider(DrillStatsTable statsProvider);

  DrillStatsTable getStatsProvider();

  void setTableMetadataProvider(TableMetadataProvider tableMetadataProvider);

  TableMetadataProvider getTableMetadataProvider();

  /**
   * Returns builder responsible for constructing required {@link TableMetadataProvider} instances
   * based on specified providers.
   *
   * @param kind kind of {@link TableMetadataProvider} whose builder should be obtained
   * @return builder responsible for constructing required {@link TableMetadataProvider}
   */
  TableMetadataProviderBuilder builder(MetadataProviderKind kind);

  /**
   * Kinds of {@link TableMetadataProvider} whose builder should be obtained.
   */
  enum MetadataProviderKind {
    PARQUET_TABLE,
    SCHEMA_STATS_ONLY
  }
}
