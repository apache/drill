/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.security;

import org.apache.drill.exec.planner.sql.SchemaUtilities;

import java.util.List;
import java.util.Objects;

/**
 * The (datasource, schema, table) triple that identifies a table in an access
 * check, following the Ranger four-level resource model
 * {@code datasource / schema / table / column}.
 *
 * <p>Use {@link #resolve(List<String>)} as the single mapping from a Calcite
 * table's qualified name to this triple. The mapping MUST be defined exactly
 * once: table-level checks (DrillCalciteCatalogReader) and column-level checks
 * (ColumnAccessChecker) must address the same resource for the same table,
 * otherwise one check may match a Ranger policy while the other does not —
 * and since Ranger denies by default, that shows up as an inconsistent
 * allow/deny depending on which check fires first.</p>
 */
public final class TableAccessResource {

  private final String dataSource;
  private final String schemaPath;
  private final String table;

  private TableAccessResource(String dataSource, String schemaPath, String table) {
    this.dataSource = dataSource;
    this.schemaPath = schemaPath;
    this.table = table;
  }

  /**
   * Resolves the access-check resource from a table's qualified name.
   *
   * <p>The first segment of a qualified name is the datasource (storage
   * plugin name), the last segment is the table, and any segments in between
   * form the schema path. The schema MUST NOT include the datasource prefix,
   * otherwise policy matching fails (policy has {@code schema=shf} but the
   * request would send {@code schema=mysql.shf}).</p>
   *
   * <p>Some backends have no schema concept (e.g. a flat file store). To keep
   * the four-level model uniform, a default schema is synthesized per
   * datasource via {@link #getDefaultSchemaByDataSource(String)}.</p>
   *
   * @param qualifiedName the Calcite-resolved qualified name (never null or
   *                      empty), e.g. {@code [mysql, shf, orders]},
   *                      {@code [cp, employee.json]} or {@code [orders]}
   * @return the resolved (datasource, schema, table) triple
   */
  public static TableAccessResource resolve(List<String> qualifiedName) {
    Objects.requireNonNull(qualifiedName, "qualifiedName must not be null");
    if (qualifiedName.isEmpty()) {
      throw new IllegalArgumentException("qualifiedName must not be empty");
    }

    String table = qualifiedName.get(qualifiedName.size() - 1);
    if (qualifiedName.size() > 2) {
      // datasource.schema.table  OR  datasource.subschema.table
      String dataSource = qualifiedName.get(0);
      String schemaPath = SchemaUtilities.getSchemaPath(
          qualifiedName.subList(1, qualifiedName.size() - 1));
      return new TableAccessResource(dataSource, schemaPath, table);
    }
    if (qualifiedName.size() == 2) {
      // datasource.table — backend has no schema; synthesize a default so the
      // four-level resource stays complete (policy matching requires a
      // non-null schema key when schema is a mandatory resource).
      String dataSource = qualifiedName.get(0);
      return new TableAccessResource(dataSource, getDefaultSchemaByDataSource(dataSource), table);
    }
    // Single-element qualified name: the table is registered at the root
    // schema. Use the table name itself as the datasource namespace.
    return new TableAccessResource(table, getDefaultSchemaByDataSource(table), table);
  }

  /**
   * Returns the default schema name to use when a table's qualified name does
   * not contain an explicit schema segment (i.e. two-segment
   * {@code datasource.table} or a single-segment fallback). This keeps the
   * four-level resource model complete even for backends that have no native
   * schema concept.
   *
   * <p>Add explicit cases below as new storage plugins are integrated. The
   * {@code default} branch returns the datasource name itself so each plugin
   * gets a distinct default schema namespace without further configuration.</p>
   *
   * @param dataSource the storage plugin / datasource name
   * @return a non-null default schema name
   */
  private static String getDefaultSchemaByDataSource(String dataSource) {
    return switch (dataSource.toLowerCase()) {
      case "dfs", "cp" -> "default";
      default -> dataSource;
    };
  }

  public String getDataSource() {
    return dataSource;
  }

  public String getSchemaPath() {
    return schemaPath;
  }

  public String getTable() {
    return table;
  }

  @Override
  public String toString() {
    return dataSource + "." + schemaPath + "." + table;
  }
}
