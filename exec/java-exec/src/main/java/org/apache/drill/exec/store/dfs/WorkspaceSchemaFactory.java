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

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.planner.sql.ExpandingConcurrentMap;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaHolder;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;
import org.apache.hadoop.fs.Path;

import com.beust.jcommander.internal.Lists;

public class WorkspaceSchemaFactory implements ExpandingConcurrentMap.MapValueFactory<String, DrillTable> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WorkspaceSchemaFactory.class);

  private ExpandingConcurrentMap<String, DrillTable> tables = new ExpandingConcurrentMap<String, DrillTable>(this);
  private final List<FormatMatcher> fileMatchers;
  private final List<FormatMatcher> dirMatchers;

  private final Path root;
  private final DrillFileSystem fs;
  private final String storageEngineName;
  private final String schemaName;
  private final FileSystemPlugin plugin;

  public WorkspaceSchemaFactory(FileSystemPlugin plugin, String schemaName, String storageEngineName, DrillFileSystem fileSystem, String path,
      List<FormatMatcher> formatMatchers) throws ExecutionSetupException {
    this.fs = fileSystem;
    this.plugin = plugin;
    this.root = new Path(path);
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

  public WorkspaceSchema create(SchemaHolder holder) {
    return new WorkspaceSchema(holder, schemaName);
  }

  @Override
  public DrillTable create(String key) {
    try {

      FileSelection fileSelection = FileSelection.create(fs, root, key);
      if(fileSelection == null) return null;
      
      if (fileSelection.containsDirectories(fs)) {
        for (FormatMatcher m : dirMatchers) {
          try {
            Object selection = m.isReadable(fileSelection);
            if (selection != null)
              return new DynamicDrillTable(plugin, storageEngineName, selection, m.getFormatPlugin().getStorageConfig());
          } catch (IOException e) {
            logger.debug("File read failed.", e);
          }
        }
        fileSelection = fileSelection.minusDirectorries(fs);
      }

      for (FormatMatcher m : fileMatchers) {
        Object selection = m.isReadable(fileSelection);
        if (selection != null)
          return new DynamicDrillTable(plugin, storageEngineName, selection, m.getFormatPlugin().getStorageConfig());
      }
      return null;

    } catch (IOException e) {
      logger.debug("Failed to create DrillTable with root {} and name {}", root, key, e);
    }

    return null;
  }

  @Override
  public void destroy(DrillTable value) {
  }

  public class WorkspaceSchema extends AbstractSchema {

    public WorkspaceSchema(SchemaHolder parentSchema, String name) {
      super(parentSchema, name);
    }

    @Override
    public Set<String> getTableNames() {
      return tables.keySet();
    }

    @Override
    public DrillTable getTable(String name) {
      return tables.get(name);
    }

  }

}
