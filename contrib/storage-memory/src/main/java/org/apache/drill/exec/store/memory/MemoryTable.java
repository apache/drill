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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.StoragePlugin;

import java.util.ArrayList;
import java.util.List;

/**
 * A table backed by memory.
 */
public class MemoryTable extends DrillTable {

    private final MemoryTableDefinition definition;

    public MemoryTable(MemoryTableDefinition definition, String storageEngineName, StoragePlugin plugin,
                       Object selection) {
        super(storageEngineName, plugin, selection);
        this.definition = definition;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return null;
    }

    /**
     * Schema in which this table resides.
     */
    public String schema() {
        return definition.schema;
    }

    /**
     * Name of the table.
     */
    public String name() {
        return definition.name;
    }

    /**
     * A list of this table's columns.
     */
    public List<MemoryColumnDefinition> columns() {
        return definition.columns;
    }

    @Override
    public String toString() {
        return "MemoryTable{" +
                "definition=" + definition +
                '}';
    }

    public static class MemoryTableDefinition {

        private final String schema;
        private final String name;
        private final List<MemoryColumnDefinition> columns;

        public MemoryTableDefinition(String schema, String name, List<MemoryColumnDefinition> columns) {
            this.schema = schema;
            this.name = name;
            this.columns = columns;
        }

        public String schema() {
            return schema;
        }

        public String name() {
            return name;
        }

        public List<MemoryColumnDefinition> columns() {
            return columns;
        }

        @Override
        public String toString() {
            return "MemoryTableDefinition{" +
                    "schema='" + schema + '\'' +
                    ", name='" + name + '\'' +
                    ", columns=" + columns +
                    '}';
        }
    }

    public static class MemoryColumnDefinition {

        private final String name;
        private final SchemaPath path;
        private final boolean nullable;
        private final List<MemoryColumnDefinition> children = new ArrayList<>();

        public MemoryColumnDefinition(String name, SchemaPath path, boolean nullable) {
            this.name = name;
            this.path = path;
            this.nullable = nullable;
        }

        public String name() {
            return name;
        }

        public SchemaPath path() {
            return path;
        }

        public boolean nullable() {
            return nullable;
        }

        public boolean hasChildren() {
            return children.size() > 0;
        }

        public List<MemoryColumnDefinition> children() {
            return children;
        }

        public void addChild(MemoryColumnDefinition memoryColumnDefinition) {
            children.add(memoryColumnDefinition);
        }

        @Override
        public String toString() {
            return "MemoryColumnDefinition{" +
                    "name='" + name + '\'' +
                    ", path=" + path +
                    ", nullable=" + nullable +
                    ", children=" + children +
                    '}';
        }
    }
}
