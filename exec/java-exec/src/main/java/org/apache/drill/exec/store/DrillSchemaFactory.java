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
package org.apache.drill.exec.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DrillSchemaFactory extends AbstractSchemaFactory {
  private static final Logger logger = LoggerFactory.getLogger(DrillSchemaFactory.class);

  private final StoragePluginRegistryImpl registry;

  public DrillSchemaFactory(String name, StoragePluginRegistryImpl registry) {
    super(name);
    this.registry = registry;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    Stopwatch watch = Stopwatch.createStarted();
    registry.registerSchemas(schemaConfig, parent);

    // Add second level schema as top level schema with name qualified with parent schema name
    // Ex: "dfs" schema has "default" and "tmp" as sub schemas. Add following extra schemas "dfs.default" and
    // "dfs.tmp" under root schema.
    //
    // Before change, schema tree looks like below:
    // "root"
    // -- "dfs"
    // -- "default"
    // -- "tmp"
    // -- "hive"
    // -- "default"
    // -- "hivedb1"
    //
    // After the change, the schema tree looks like below:
    // "root"
    // -- "dfs"
    // -- "default"
    // -- "tmp"
    // -- "dfs.default"
    // -- "dfs.tmp"
    // -- "hive"
    // -- "default"
    // -- "hivedb1"
    // -- "hive.default"
    // -- "hive.hivedb1"
    List<SchemaPlus> secondLevelSchemas = new ArrayList<>();
    for (String firstLevelSchemaName : parent.getSubSchemaNames()) {
      SchemaPlus firstLevelSchema = parent.getSubSchema(firstLevelSchemaName);
      for (String secondLevelSchemaName : firstLevelSchema.getSubSchemaNames()) {
        secondLevelSchemas.add(firstLevelSchema.getSubSchema(secondLevelSchemaName));
      }
    }

    for (SchemaPlus schema : secondLevelSchemas) {
      AbstractSchema drillSchema;
      try {
        drillSchema = schema.unwrap(AbstractSchema.class);
      } catch (ClassCastException e) {
        throw new RuntimeException(String.format("Schema '%s' is not expected under root schema", schema.getName()));
      }
      SubSchemaWrapper wrapper = new SubSchemaWrapper(drillSchema);
      parent.add(wrapper.getName(), wrapper);
    }

    logger.debug("Took {} ms to register schemas.", watch.elapsed(TimeUnit.MILLISECONDS));
  }
}
