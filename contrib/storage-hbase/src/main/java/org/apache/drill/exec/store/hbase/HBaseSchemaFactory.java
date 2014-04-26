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
package org.apache.drill.exec.store.hbase;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;

import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.rpc.user.DrillUser;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.google.common.collect.Sets;

public class HBaseSchemaFactory implements SchemaFactory {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseSchemaFactory.class);

  final String schemaName;
  final HBaseStoragePlugin plugin;

  public HBaseSchemaFactory(HBaseStoragePlugin plugin, String name) throws IOException {
    this.plugin = plugin;
    this.schemaName = name;
  }

  @Override
  public void registerSchemas(DrillUser user, SchemaPlus parent) {
    HBaseSchema schema = new HBaseSchema(schemaName);
    SchemaPlus hPlus = parent.add(schemaName, schema);
    schema.setHolder(hPlus);
  }

  class HBaseSchema extends AbstractSchema {

    public HBaseSchema(String name) {
      super(name);
    }

    public void setHolder(SchemaPlus plusOfThis) {
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
    public DrillTable getTable(String name) {
      Object selection = new HTableReadEntry(name);
      return new DynamicDrillTable(plugin, schemaName, selection, plugin.getConfig());
    }

    @Override
    public Set<String> getTableNames() {
      try(HBaseAdmin admin = new HBaseAdmin(plugin.getConfig().conf)) {
        HTableDescriptor[] tables = admin.listTables();
        Set<String> tableNames = Sets.newHashSet();
        for (HTableDescriptor table : tables) {
          tableNames.add(new String(table.getName()));
        }
        return tableNames;
      } catch (Exception e) {
        logger.warn("Failure while loading table names for database '{}'.", schemaName, e.getCause());
        return Collections.emptySet();
      }
    }
  }
}
