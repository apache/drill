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
package org.apache.drill.exec.store.memory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import org.apache.drill.exec.planner.logical.CreateTableEntry;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Schema factory for memory storage plugin.
 */
public class MemorySchemaFactory implements SchemaFactory {

    private final String schema;
    private final MemoryStoragePlugin plugin;
    private final MemorySchemaProvider provider;

    public MemorySchemaFactory(MemoryStoragePlugin plugin, String schema) {
        this.plugin = plugin;
        this.schema = schema;
        this.provider = new MemorySchemaProvider();
        logger.info("Factory created for memory storage plugin: [{}]", schema);
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
        MemorySchema m = new MemorySchema(schema);
        SchemaPlus plus = parent.add(schema, m);
        m.setHolder(plus);
        logger.info("Schema registered for memory storage plugin: [{}]", schema);
    }

    // XXX - Remove this and make a proper writer interface.
    public MemorySchemaProvider getSchemaProvider() {
        return provider;
    }

    public class MemorySchema extends AbstractSchema {

        public MemorySchema(String name) {
            super(ImmutableList.<String>of(), name);
        }

        public MemorySchema(List<String> parentSchemaPath, String name) {
            super(parentSchemaPath, name);
        }

        @Override
        public String getTypeName() {
            return MemoryStoragePluginConfig.NAME;
        }

        public void setHolder(SchemaPlus plusOfThis) {
            for (String s : getSubSchemaNames()) {
                plusOfThis.add(s, getSubSchema(s));
            }
        }

        @Override
        public AbstractSchema getSubSchema(String name) {
            if (!provider.exists(name)) {
                logger.info("Schema: [{}] does not exist in memory", name);
                return null;
            }
            Set<String> tables = provider.tables(name);
            return new MemoryObjectSchema(this, name, tables);
        }

        @Override
        public Set<String> getSubSchemaNames() {
            return Sets.newHashSet(provider.schemas());
        }

        @Override
        public Set<String> getTableNames() {
            return provider.tables(schema);
        }

        @Override
        public CreateTableEntry createNewTable(String tableName, List<String> partitionColumns) {
            return super.createNewTable(tableName, partitionColumns);
        }
    }

    public class MemoryObjectSchema extends MemorySchema {

        private final Set<String> tables;

        public MemoryObjectSchema(MemorySchema memorySchema, String name, Set<String> tables) {
            super(memorySchema.getSchemaPath(), name);
            this.tables = tables;
        }

        @Override
        public String getTypeName() {
            return MemoryStoragePluginConfig.NAME;
        }

        @Override
        public Table getTable(final String table) {

            MemoryTable.MemoryTableDefinition definition = provider.table(schema, table);

            Object spec = null;

            MemoryTable mt = new MemoryTable(definition, schema, plugin, spec);

            return mt;
        }

        @Override
        public Set<String> getTableNames() {
            return tables;
        }
    }
}
