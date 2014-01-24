/**
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
package org.apache.drill.sql.client.full;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.store.SchemaProvider;
import org.apache.drill.exec.store.hive.HiveStorageEngineConfig;
import org.apache.drill.exec.store.hive.HiveStorageEngine.HiveSchemaProvider;

import org.apache.drill.jdbc.DrillTable;
import org.apache.hadoop.hive.ql.metadata.Hive;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class HiveSchema implements Schema{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveSchema.class);

  private final JavaTypeFactory typeFactory;
  private final Schema parentSchema;
  private final String name;
  private final Expression expression;
  private final QueryProvider queryProvider;
  private final HiveSchemaProvider schemaProvider;
  private final DrillClient client;
  private final HiveStorageEngineConfig config;
  private Hive hiveDb;

  public HiveSchema(DrillClient client, StorageEngineConfig config, SchemaProvider schemaProvider,
                    JavaTypeFactory typeFactory, Schema parentSchema, String name,
                    Expression expression, QueryProvider queryProvider) {
    super();
    this.client = client;
    this.typeFactory = typeFactory;
    this.parentSchema = parentSchema;
    this.name = name;
    this.expression = expression;
    this.queryProvider = queryProvider;
    this.schemaProvider = (HiveSchemaProvider) schemaProvider;
    this.config = (HiveStorageEngineConfig) config;
  }

  public Hive getHiveDb() {
    try {
      if (hiveDb == null) {
        this.hiveDb = Hive.get(this.config.getHiveConf());
      }
    } catch(HiveException ex) {
      throw new RuntimeException("Failed to create Hive MetaStore Client", ex);
    }

    return hiveDb;
  }

  /**
   * fetch the database details from
   */
  @Override
  public Schema getSubSchema(String name) {
    return new HiveDatabaseSchema(client, config, schemaProvider, typeFactory, this,
      name, expression, queryProvider);
  }

  @Override
  public JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }

  @Override
  public Schema getParentSchema() {
    return parentSchema;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Expression getExpression() {
    return expression;
  }

  @Override
  public QueryProvider getQueryProvider() {
    return queryProvider;
  }

  @Override
  public Collection<TableFunctionInSchema> getTableFunctions(String name) {
    return Collections.EMPTY_LIST;
  }

  @Override
  public Multimap<String, TableFunctionInSchema> getTableFunctions() {
    return ArrayListMultimap.create();
  }

  /**
   * return all databases in metastore
   */
  @Override
  public Collection<String> getSubSchemaNames() {
    try {
      return getHiveDb().getAllDatabases();
    } catch(HiveException ex) {
      throw new RuntimeException("Failed to get databases from Hive MetaStore", ex);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <E> Table<E> getTable(String name, Class<E> elementType) {
    return null;
  }

  /**
   * Fetch the databases and tables in each database
   * @return
   */
  @Override
  public Map<String, TableInSchema> getTables() {
    Map<String, TableInSchema> allTables = Maps.newHashMap();

    Collection<String> dbs = getSubSchemaNames();
    for(String db : dbs) {
      // create a sub schema for each database and query the tables within it
      HiveDatabaseSchema dbSchema = new HiveDatabaseSchema(client, config, schemaProvider,
        typeFactory, this, db, expression, queryProvider);
      allTables.putAll(dbSchema.getTables());
    }

    return allTables;
  }
}
