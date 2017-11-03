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
package org.apache.drill.exec.planner.sql;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.SimpleCalciteSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginRegistry;


/**
 * Unlike SimpleCalciteSchema, DynamicSchema could have an empty or partial schemaMap, but it could maintain a map of
 * name->SchemaFactory, and only register schema when the corresponsdent name is requested.
 */
public class DynamicSchema extends SimpleCalciteSchema {

  public DynamicSchema(CalciteSchema parent, Schema schema, String name) {
    super(parent, schema, name);
  }

  @Override
  public CalciteSchema getSubSchema(String schemaName, boolean caseSensitive) {
    Schema s = schema.getSubSchema(schemaName);
    if (s != null) {
      return new DynamicSchema(this, s, schemaName);
    }
    CalciteSchema ret = getSubSchemaMap().get(schemaName);
    return ret;
  }

  @Override
  public SchemaPlus plus() {
    return super.plus();
  }

  public static SchemaPlus createRootSchema(StoragePluginRegistry storages, SchemaConfig schemaConfig) {
    DynamicRootSchema rootSchema = new DynamicRootSchema(storages, schemaConfig);
    return rootSchema.plus();
  }

}
