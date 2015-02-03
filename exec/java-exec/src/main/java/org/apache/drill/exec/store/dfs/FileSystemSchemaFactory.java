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
package org.apache.drill.exec.store.dfs;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.hydromatic.optiq.Function;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Table;

import org.apache.drill.exec.planner.logical.CreateTableEntry;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.PartitionNotFoundException;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory.WorkspaceSchema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;


/**
 * This is the top level schema that responds to root level path requests. Also supports
 */
public class FileSystemSchemaFactory implements SchemaFactory{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemSchemaFactory.class);

  private List<WorkspaceSchemaFactory> factories;
  private String schemaName;
  private final String defaultSchemaName = "default";


  public FileSystemSchemaFactory(String schemaName, List<WorkspaceSchemaFactory> factories) {
    super();
    this.schemaName = schemaName;
    this.factories = factories;
  }

  @Override
  public void registerSchemas(UserSession session, SchemaPlus parent) {
    FileSystemSchema schema = new FileSystemSchema(schemaName, session);
    SchemaPlus plusOfThis = parent.add(schema.getName(), schema);
    schema.setPlus(plusOfThis);
  }

  public class FileSystemSchema extends AbstractSchema {

    private final WorkspaceSchema defaultSchema;
    private final Map<String, WorkspaceSchema> schemaMap = Maps.newHashMap();

    public FileSystemSchema(String name, UserSession session) {
      super(ImmutableList.<String>of(), name);
      for(WorkspaceSchemaFactory f :  factories){
        WorkspaceSchema s = f.createSchema(getSchemaPath(), session);
        schemaMap.put(s.getName(), s);
      }

      defaultSchema = schemaMap.get(defaultSchemaName);
    }

    void setPlus(SchemaPlus plusOfThis){
      for(WorkspaceSchema s : schemaMap.values()){
        plusOfThis.add(s.getName(), s);
      }
    }

    @Override
    public Iterable<String> getSubPartitions(String table,
                                             List<String> partitionColumns,
                                             List<String> partitionValues
                                            ) throws PartitionNotFoundException {
      List<FileStatus> fileStatuses;
      try {
        fileStatuses = defaultSchema.getFS().list(false, new Path(defaultSchema.getDefaultLocation(), table));
      } catch (IOException e) {
        throw new PartitionNotFoundException("Error finding partitions for table " + table, e);
      }
      return new SubDirectoryList(fileStatuses);
    }

    @Override
    public boolean showInInformationSchema() {
      return false;
    }

    @Override
    public String getTypeName() {
      return FileSystemConfig.NAME;
    }

    @Override
    public Table getTable(String name) {
      return defaultSchema.getTable(name);
    }

    @Override
    public Collection<Function> getFunctions(String name) {
      return defaultSchema.getFunctions(name);
    }

    @Override
    public Set<String> getFunctionNames() {
      return defaultSchema.getFunctionNames();
    }

    @Override
    public AbstractSchema getSubSchema(String name) {
      return schemaMap.get(name);
    }

    @Override
    public Set<String> getSubSchemaNames() {
      return schemaMap.keySet();
    }

    @Override
    public Set<String> getTableNames() {
      return defaultSchema.getTableNames();
    }

    @Override
    public boolean isMutable() {
      return defaultSchema.isMutable();
    }

    @Override
    public CreateTableEntry createNewTable(String tableName) {
      return defaultSchema.createNewTable(tableName);
    }

    @Override
    public AbstractSchema getDefaultSchema() {
      return defaultSchema;
    }
  }
}
