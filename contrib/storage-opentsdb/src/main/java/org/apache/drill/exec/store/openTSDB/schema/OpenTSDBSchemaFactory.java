/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.openTSDB.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.CreateTableEntry;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.openTSDB.DrillOpenTSDBTable;
import org.apache.drill.exec.store.openTSDB.OpenTSDBScanSpec;
import org.apache.drill.exec.store.openTSDB.OpenTSDBStoragePlugin;
import org.apache.drill.exec.store.openTSDB.OpenTSDBStoragePluginConfig;
import org.apache.drill.exec.store.openTSDB.client.Schema;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class OpenTSDBSchemaFactory implements SchemaFactory {

    final private String schemaName;
    private OpenTSDBStoragePlugin plugin;

    public OpenTSDBSchemaFactory(OpenTSDBStoragePlugin plugin, String schemaName) {
        this.plugin = plugin;
        this.schemaName = schemaName;
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
        OpenTSDBTables schema = new OpenTSDBTables(schemaName);
        parent.add(schemaName, schema);
    }


    class OpenTSDBTables extends AbstractSchema {

        private final Map<String, OpenTSDBDatabaseSchema> schemaMap = Maps.newHashMap();

        public OpenTSDBTables(String name) {
            super(ImmutableList.<String>of(), name);
        }

        @Override
        public AbstractSchema getSubSchema(String name) {
            Set<String> tables;
            try {
                if (!schemaMap.containsKey(name)) {
                    tables = plugin.getClient().getAllTablesName().execute().body();
                    schemaMap.put(name, new OpenTSDBDatabaseSchema(tables, this, name));
                }
                return schemaMap.get(name);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }

        @Override
        public Set<String> getSubSchemaNames() {
            return Collections.emptySet();
        }

        @Override
        public Table getTable(String name) {
            OpenTSDBScanSpec scanSpec = new OpenTSDBScanSpec(name);
            try {
                return new DrillOpenTSDBTable(schemaName, plugin, new Schema(), scanSpec);
            } catch (Exception e) {
                logger.warn("Failure while retrieving openTSDB table {}", name, e);
                return null;
            }
        }

        @Override
        public Set<String> getTableNames() {
            try {
                return plugin.getClient().getAllTablesName().execute().body();
            } catch (Exception e) {
                logger.warn("Failure reading openTSDB tables.", e);
                return Collections.emptySet();
            }
        }

        @Override
        public CreateTableEntry createNewTable(final String tableName, List<String> partitionColumns) {
            return null;
        }

        @Override
        public void dropTable(String tableName) {
        }

        @Override
        public boolean isMutable() {
            return true;
        }

        @Override
        public String getTypeName() {
            return OpenTSDBStoragePluginConfig.NAME;
        }

        DrillTable getDrillTable(String collectionName) {
            OpenTSDBScanSpec openTSDBScanSpec = new OpenTSDBScanSpec(collectionName);
            return new DynamicDrillTable(plugin, schemaName, null, openTSDBScanSpec);
        }
    }
}