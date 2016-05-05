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
package org.apache.drill.exec.store.kudu;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Writer;
import org.apache.drill.exec.planner.logical.CreateTableEntry;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
import org.kududb.Schema;
import org.kududb.client.KuduTable;
import org.kududb.client.ListTablesResponse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

public class KuduSchemaFactory implements SchemaFactory {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KuduSchemaFactory.class);

  final String schemaName;
  final KuduStoragePlugin plugin;

  public KuduSchemaFactory(KuduStoragePlugin plugin, String name) throws IOException {
    this.plugin = plugin;
    this.schemaName = name;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    KuduTables schema = new KuduTables(schemaName);
    SchemaPlus hPlus = parent.add(schemaName, schema);
    schema.setHolder(hPlus);
  }

  class KuduTables extends AbstractSchema {

    public KuduTables(String name) {
      super(ImmutableList.<String>of(), name);
    }

    public void setHolder(SchemaPlus plusOfThis) {
    }

    @Override
    public AbstractSchema getSubSchema(String name) {
      return null;
    }

    @Override
    public Set<String> getSubSchemaNames() {
      return Collections.emptySet();
    }

    @Override
    public Table getTable(String name) {
      KuduScanSpec scanSpec = new KuduScanSpec(name);
      try {
        KuduTable table = plugin.getClient().openTable(name);
        Schema schema = table.getSchema();
        return new DrillKuduTable(schemaName, plugin, schema, scanSpec);
      } catch (Exception e) {
        logger.warn("Failure while retrieving kudu table {}", name, e);
        return null;
      }

    }

    @Override
    public Set<String> getTableNames() {
      try {
        ListTablesResponse tablesList = plugin.getClient().getTablesList();
        return Sets.newHashSet(tablesList.getTablesList());
      } catch (Exception e) {
        logger.warn("Failure reading kudu tables.", e);
        return Collections.emptySet();
      }
    }

    @Override
    public CreateTableEntry createNewTable(final String tableName, List<String> partitionColumns) {
      return new CreateTableEntry(){

        @Override
        public Writer getWriter(PhysicalOperator child) throws IOException {
          return new KuduWriter(child, tableName, plugin);
        }

        @Override
        public List<String> getPartitionColumns() {
          return Collections.emptyList();
        }

      };
    }

    @Override
    public void dropTable(String tableName) {
      try {
        plugin.getClient().deleteTable(tableName);
      } catch (Exception e) {
        throw UserException.dataWriteError(e)
            .message("Failure while trying to drop table '%s'.", tableName)
            .addContext("plugin", name)
            .build(logger);
      }
    }

    @Override
    public boolean isMutable() {
      return true;
    }

    @Override
    public String getTypeName() {
      return KuduStoragePluginConfig.NAME;
    }

  }

}
