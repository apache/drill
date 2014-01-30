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
package org.apache.drill.exec.planner.sql;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import net.hydromatic.linq4j.expressions.DefaultExpression;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.TableFunction;

import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.SchemaProvider;

public class FileSystemSchema implements Schema, ExpandingConcurrentMap.MapValueFactory<String, DrillTable>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemSchema.class);
  
  private ExpandingConcurrentMap<String, DrillTable> tables = new ExpandingConcurrentMap<String, DrillTable>(this);
  
  private final SchemaPlus parentSchema;
  private final String name;
  private final Expression expression = new DefaultExpression(Object.class);
  private final SchemaProvider schemaProvider;
  private final StorageEngineConfig config;
  
  public FileSystemSchema(StorageEngineConfig config, SchemaProvider schemaProvider, SchemaPlus parentSchema, String name) {
    super();
    this.parentSchema = parentSchema;
    this.name = name;
    this.schemaProvider = schemaProvider;
    this.config = config;
  }

  @Override
  public Schema getSubSchema(String name) {
    return null;
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
  public Collection<TableFunction> getTableFunctions(String name) {
    return Collections.emptyList();
  }
  
  @Override
  public SchemaPlus getParentSchema() {
    return parentSchema;
  }

  @Override
  public Set<String> getTableNames() {
    return tables.keySet();
  }

  @Override
  public Set<String> getTableFunctionNames() {
    return Collections.emptySet();
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return Collections.emptySet();
  }

  @Override
  public boolean isMutable() {
    return true;
  }
  
  @Override
  public DrillTable getTable(String name) {
    return tables.get(name);
  }

  @Override
  public DrillTable create(String key) {
    Object selection = schemaProvider.getSelectionBaseOnName(key);
    if(selection == null) return null;
    
    return new DrillTable(name, this.name, selection, config);
  }

  @Override
  public void destroy(DrillTable value) {
  }

  
  
}
