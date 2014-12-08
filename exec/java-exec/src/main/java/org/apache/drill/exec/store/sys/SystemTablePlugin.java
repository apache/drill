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
package org.apache.drill.exec.store.sys;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import net.hydromatic.optiq.SchemaPlus;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.DrillConfigIterator;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.AbstractStoragePlugin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class SystemTablePlugin extends AbstractStoragePlugin{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SystemTablePlugin.class);

  private final DrillbitContext context;
  private final String name;

  public SystemTablePlugin(SystemTablePluginConfig configuration, DrillbitContext context, String name){
    this.context = context;
    this.name = name;
  }

  private SystemSchema schema = new SystemSchema();

  @Override
  public StoragePluginConfig getConfig() {
    return SystemTablePluginConfig.INSTANCE;
  }

  @Override
  public void registerSchemas(UserSession session, SchemaPlus parent) {
    parent.add(schema.getName(), schema);
  }

  public Iterator<Object> getRecordIterator(FragmentContext context, SystemTable table){
    switch(table){
    case VERSION:
      return new VersionIterator();
    case DRILLBITS:
      return new DrillbitIterator(context);
    case OPTION:
      return Iterables.concat((Iterable<Object>)(Object) new DrillConfigIterator(context.getConfig()), //
          context.getOptions()).iterator();
    default:
      throw new UnsupportedOperationException("Unable to create record iterator for table: " + table.getTableName());
    }
  }


  @Override
  public AbstractGroupScan getPhysicalScan(JSONOptions selection, List<SchemaPath> columns) throws IOException {
    SystemTable table = selection.getWith(context.getConfig(), SystemTable.class);
    return new SystemTableScan(table, this);
  }

  private class SystemSchema extends AbstractSchema{

    private Set<String> tableNames;

    public SystemSchema() {
      super(ImmutableList.<String>of(), "sys");
      Set<String> names = Sets.newHashSet();
      for(SystemTable t : SystemTable.values()){
        names.add(t.getTableName());
      }
      this.tableNames = ImmutableSet.copyOf(names);
    }

    @Override
    public Set<String> getTableNames() {
      return tableNames;
    }


    @Override
    public DrillTable getTable(String name) {
      for(SystemTable table : SystemTable.values()){
        if(table.getTableName().equalsIgnoreCase(name)){
          return new StaticDrillTable(table.getType(), SystemTablePlugin.this.name, SystemTablePlugin.this, table);
        }
      }
      return null;

    }

    @Override
    public String getTypeName() {
      return SystemTablePluginConfig.NAME;
    }

  }
}
