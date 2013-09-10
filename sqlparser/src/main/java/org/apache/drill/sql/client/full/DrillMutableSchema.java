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

import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Multimap;

import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

public class DrillMutableSchema implements Schema {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillMutableSchema.class);

  @Override
  public Schema getParentSchema() {
    return null;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public Collection<TableFunctionInSchema> getTableFunctions(String name) {
    return null;
  }

  @Override
  public <E> Table<E> getTable(String name, Class<E> elementType) {
    return null;
  }

  @Override
  public Expression getExpression() {
    return null;
  }

  @Override
  public QueryProvider getQueryProvider() {
    return null;
  }

  @Override
  public Multimap<String, TableFunctionInSchema> getTableFunctions() {
    return null;
  }

  @Override
  public Collection<String> getSubSchemaNames() {
    return null;
  }

  @Override
  public Map<String, TableInSchema> getTables() {
    return null;
  }

  @Override
  public Schema getSubSchema(String name) {
    return null;
  }

  @Override
  public JavaTypeFactory getTypeFactory() {
    return null;
  }


}
