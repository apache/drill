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
package org.apache.drill.exec.store;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import net.hydromatic.linq4j.expressions.DefaultExpression;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.TableFunction;

import org.apache.drill.exec.planner.logical.DrillTable;

public abstract class AbstractSchema implements Schema{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractSchema.class);

  private final SchemaHolder parentSchema;

  protected final String name;
  private static final Expression EXPRESSION = new DefaultExpression(Object.class);

  public AbstractSchema(SchemaHolder parentSchema, String name) {
    super();
    this.parentSchema = parentSchema;
    this.name = name;
  }

  
  @Override
  public SchemaPlus getParentSchema() {
    return parentSchema.getSchema();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Collection<TableFunction> getTableFunctions(String name) {
    return Collections.emptyList();
  }

  @Override
  public Set<String> getTableFunctionNames() {
    return Collections.emptySet();
  }
  
  

  @Override
  public Schema getSubSchema(String name) {
    return null;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return Collections.emptySet();
  }

  @Override
  public Expression getExpression() {
    return EXPRESSION;
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  public DrillTable getTable(String name){
    return null;
  }

  @Override
  public Set<String> getTableNames() {
    return Collections.emptySet();
  }
  
  
  
}
