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
import java.util.List;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import net.hydromatic.optiq.Table;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.planner.logical.CreateTableEntry;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.planner.logical.FileSystemCreateTableEntry;
import org.apache.drill.exec.planner.sql.ExpandingConcurrentMap;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.options.OptionValidator;
import org.apache.drill.exec.server.options.TypeValidators.StringValidator;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;
import org.apache.hadoop.fs.Path;

public class WorkspaceSchemaFactory implements ExpandingConcurrentMap.MapValueFactory<String, DrillTable> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WorkspaceSchemaFactory.class);

  private final List<FormatMatcher> fileMatchers;
  private final List<FormatMatcher> dirMatchers;

  private final WorkspaceConfig config;
  private final DrillFileSystem fs;
  private final String storageEngineName;
  private final String schemaName;
  private final FileSystemPlugin plugin;

  public WorkspaceSchemaFactory(FileSystemPlugin plugin, String schemaName, String storageEngineName,
      DrillFileSystem fileSystem, WorkspaceConfig config,
      List<FormatMatcher> formatMatchers) throws ExecutionSetupException {
    this.fs = fileSystem;
    this.plugin = plugin;
    this.config = config;
    this.fileMatchers = Lists.newArrayList();
    this.dirMatchers = Lists.newArrayList();
    for (FormatMatcher m : formatMatchers) {
      if (m.supportDirectoryReads()) {
        dirMatchers.add(m);
      }
      fileMatchers.add(m);
    }
    this.storageEngineName = storageEngineName;
    this.schemaName = schemaName;
  }

  public WorkspaceSchema createSchema(List<String> parentSchemaPath, UserSession session) {
    return new WorkspaceSchema(parentSchemaPath, schemaName, session);
  }

  @Override
  public DrillTable create(String key) {
    try {

      FileSelection fileSelection = FileSelection.create(fs, config.getLocation(), key);
      if(fileSelection == null) return null;

      if (fileSelection.containsDirectories(fs)) {
        for (FormatMatcher m : dirMatchers) {
          try {
            Object selection = m.isReadable(fileSelection);
            if (selection != null)
              return new DynamicDrillTable(plugin, storageEngineName, selection);
          } catch (IOException e) {
            logger.debug("File read failed.", e);
          }
        }
        fileSelection = fileSelection.minusDirectories(fs);
      }

      for (FormatMatcher m : fileMatchers) {
        Object selection = m.isReadable(fileSelection);
        if (selection != null)
          return new DynamicDrillTable(plugin, storageEngineName, selection);
      }
      return null;

    } catch (IOException e) {
      logger.debug("Failed to create DrillTable with root {} and name {}", config.getLocation(), key, e);
    }

    return null;
  }

  @Override
  public void destroy(DrillTable value) {
  }

  public class WorkspaceSchema extends AbstractSchema {

    private ExpandingConcurrentMap<String, DrillTable> tables = new ExpandingConcurrentMap<String, DrillTable>(WorkspaceSchemaFactory.this);
    private UserSession session;

    public WorkspaceSchema(List<String> parentSchemaPath, String name, UserSession session) {
      super(parentSchemaPath, name);
      this.session = session;
    }

    @Override
    public Set<String> getTableNames() {
      return Sets.union(tables.keySet(), session.getViewStore().getViews(Joiner.on(".").join(getSchemaPath())).keySet());
    }

    @Override
    public Table getTable(String name) {
      if (tables.containsKey(name))
        return tables.get(name);

      return session.getViewStore().getViews(Joiner.on(".").join(getSchemaPath())).get(name);
    }

    @Override
    public boolean isMutable() {
      return config.isWritable();
    }

    public DrillFileSystem getFS() {
      return fs;
    }

    public String getDefaultLocation() {
      return config.getLocation();
    }


    @Override
    public CreateTableEntry createNewTable(String tableName) {
      String storage = session.getOptions().getOption(ExecConstants.OUTPUT_FORMAT_OPTION).string_val;
      FormatPlugin formatPlugin = plugin.getFormatPlugin(storage);
      if (formatPlugin == null)
        throw new UnsupportedOperationException(
          String.format("Unsupported format '%s' in workspace '%s'", config.getStorageFormat(),
              Joiner.on(".").join(getSchemaPath())));

      return new FileSystemCreateTableEntry(
          (FileSystemConfig) plugin.getConfig(),
          formatPlugin,
          config.getLocation() + Path.SEPARATOR + tableName);
    }

    @Override
    public String getTypeName() {
      return FileSystemConfig.NAME;
    }
  }

}
