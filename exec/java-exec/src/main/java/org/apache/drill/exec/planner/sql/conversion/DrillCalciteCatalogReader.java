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
package org.apache.drill.exec.planner.sql.conversion;

import java.util.List;
import java.util.Properties;
import java.util.function.BooleanSupplier;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.DynamicSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.metastore.MetadataProviderManager;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.sql.SchemaUtilities;
import org.apache.drill.exec.rpc.user.UserSession;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extension of {@link CalciteCatalogReader} to add ability to check for temporary tables first
 * if schema is not indicated near table name during query parsing
 * or indicated workspace is default temporary workspace.
 */
class DrillCalciteCatalogReader extends CalciteCatalogReader {

  private static final Logger logger = LoggerFactory.getLogger(DrillCalciteCatalogReader.class);

  private final DrillConfig drillConfig;
  private final UserSession session;
  private boolean allowTemporaryTables;
  private final BooleanSupplier useRootSchema;

  private final LoadingCache<DrillTableKey, MetadataProviderManager> tableCache;

  DrillCalciteCatalogReader(SchemaPlus rootSchema,
                            boolean caseSensitive,
                            List<String> defaultSchema,
                            JavaTypeFactory typeFactory,
                            DrillConfig drillConfig,
                            UserSession session,
                            BooleanSupplier useRootSchema) {
    super(DynamicSchema.from(rootSchema), SqlNameMatchers.withCaseSensitive(caseSensitive),
      ImmutableList.of(defaultSchema, ImmutableList.of()),
      typeFactory, getConnectionConfig(caseSensitive));
    this.drillConfig = drillConfig;
    this.session = session;
    this.allowTemporaryTables = true;
    this.tableCache =
        CacheBuilder.newBuilder()
          .build(new CacheLoader<DrillTableKey, MetadataProviderManager>() {
            @Override
            public MetadataProviderManager load(DrillTableKey key) {
              return key.getMetadataProviderManager();
            }
          });
    this.useRootSchema = useRootSchema;
  }

  /**
   * Disallow temporary tables presence in sql statement (ex: in view definitions)
   */
  void disallowTemporaryTables() {
    this.allowTemporaryTables = false;
  }

  /**
   * If schema is not indicated (only one element in the list) or schema is default temporary workspace,
   * we need to check among session temporary tables in default temporary workspace first.
   * If temporary table is found and temporary tables usage is allowed, its table instance will be returned,
   * otherwise search will be conducted in original workspace.
   *
   * @param names list of schema and table names, table name is always the last element
   * @return table instance, null otherwise
   * @throws UserException if temporary tables usage is disallowed
   */
  @Override
  public Prepare.PreparingTable getTable(List<String> names) {
    checkTemporaryTable(names);
    Prepare.PreparingTable table = super.getTable(names);
    DrillTable drillTable;
    if (table != null && (drillTable = table.unwrap(DrillTable.class)) != null) {
      drillTable.setOptions(session.getOptions());
      drillTable.setTableMetadataProviderManager(tableCache.getUnchecked(DrillTableKey.of(names, drillTable)));
    }
    return table;
  }

  private void checkTemporaryTable(List<String> names) {
    if (allowTemporaryTables || !needsTemporaryTableCheck(names, session.getDefaultSchemaPath(), drillConfig)) {
      return;
    }
    String tableName = names.get(names.size() - 1);
    String originalTableName = session.resolveTemporaryTableName(tableName);
    if (originalTableName != null) {
      throw UserException
          .validationError()
          .message("A reference to temporary table [%s] was made in a context where temporary table references are not allowed.", tableName)
          .build(logger);
    }
  }

  @Override
  public List<List<String>> getSchemaPaths() {
    if (useRootSchema.getAsBoolean()) {
      return ImmutableList.of(ImmutableList.of());
    }
    return super.getSchemaPaths();
  }

  /**
   * We should check if passed table is temporary or not if:
   * <li>schema is not indicated (only one element in the names list)<li/>
   * <li>current schema or indicated schema is default temporary workspace<li/>
   *
   * Examples (where dfs.tmp is default temporary workspace):
   * <li>select * from t<li/>
   * <li>select * from dfs.tmp.t<li/>
   * <li>use dfs; select * from tmp.t<li/>
   *
   * @param names             list of schema and table names, table name is always the last element
   * @param defaultSchemaPath current schema path set using USE command
   * @param drillConfig       drill config
   * @return true if check for temporary table should be done, false otherwise
   */
  private boolean needsTemporaryTableCheck(List<String> names, String defaultSchemaPath, DrillConfig drillConfig) {
    if (names.size() == 1) {
      return true;
    }
    String schemaPath = SchemaUtilities.getSchemaPath(names.subList(0, names.size() - 1));
    return SchemaUtilities.isTemporaryWorkspace(schemaPath, drillConfig) ||
        SchemaUtilities.isTemporaryWorkspace(
            SchemaUtilities.SCHEMA_PATH_JOINER.join(defaultSchemaPath, schemaPath), drillConfig);
  }

  /**
   * Creates {@link CalciteConnectionConfigImpl} instance with specified caseSensitive property.
   *
   * @param caseSensitive is case sensitive.
   * @return {@link CalciteConnectionConfigImpl} instance
   */
  private static CalciteConnectionConfigImpl getConnectionConfig(boolean caseSensitive) {
    Properties properties = new Properties();
    properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
        String.valueOf(caseSensitive));
    return new CalciteConnectionConfigImpl(properties);
  }
}
