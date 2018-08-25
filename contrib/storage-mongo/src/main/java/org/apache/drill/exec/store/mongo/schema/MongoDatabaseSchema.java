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
package org.apache.drill.exec.store.mongo.schema;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.schema.Table;

import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.mongo.MongoStoragePluginConfig;
import org.apache.drill.exec.store.mongo.schema.MongoSchemaFactory.MongoSchema;

public class MongoDatabaseSchema extends AbstractSchema {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(MongoDatabaseSchema.class);
  private final MongoSchema mongoSchema;
  private final Set<String> tableNames;

  private final Map<String, DrillTable> drillTables = new HashMap<>();

  public MongoDatabaseSchema(List<String> tableList, MongoSchema mongoSchema,
      String name) {
    super(mongoSchema.getSchemaPath(), name);
    this.mongoSchema = mongoSchema;
    this.tableNames = new HashSet<>(tableList);
  }

  @Override
  public Table getTable(String tableName) {
    if (!tableNames.contains(tableName)) { // table does not exist
      return null;
    }

    if (! drillTables.containsKey(tableName)) {
      drillTables.put(tableName, mongoSchema.getDrillTable(this.name, tableName));
    }

    return drillTables.get(tableName);

  }

  @Override
  public Set<String> getTableNames() {
    return tableNames;
  }

  @Override
  public String getTypeName() {
    return MongoStoragePluginConfig.NAME;
  }

}
