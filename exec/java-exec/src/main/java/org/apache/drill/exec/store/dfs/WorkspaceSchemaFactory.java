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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.hydromatic.optiq.Table;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.cache.DistributedCache.CacheConfig;
import org.apache.drill.exec.dotdrill.DotDrillFile;
import org.apache.drill.exec.dotdrill.DotDrillType;
import org.apache.drill.exec.dotdrill.DotDrillUtil;
import org.apache.drill.exec.dotdrill.View;
import org.apache.drill.exec.planner.logical.CreateTableEntry;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DrillViewTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.planner.logical.FileSystemCreateTableEntry;
import org.apache.drill.exec.planner.sql.ExpandingConcurrentMap;
import org.apache.drill.exec.planner.sql.ExpandingConcurrentMap.MapValueFactory;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;
import org.apache.drill.exec.store.dfs.shim.DrillInputStream;
import org.apache.drill.exec.store.dfs.shim.DrillOutputStream;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.apache.drill.exec.store.sys.PStoreProvider;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class WorkspaceSchemaFactory implements ExpandingConcurrentMap.MapValueFactory<String, DrillTable> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WorkspaceSchemaFactory.class);

  private final List<FormatMatcher> fileMatchers;
  private final List<FormatMatcher> dirMatchers;

  private final WorkspaceConfig config;
  private final DrillFileSystem fs;
  private final DrillConfig drillConfig;
  private final String storageEngineName;
  private final String schemaName;
  private final FileSystemPlugin plugin;
//  private final PTable<String> knownPaths;

  private final PStore<String> knownViews;
  private final ObjectMapper mapper;



  public WorkspaceSchemaFactory(DrillConfig drillConfig, PStoreProvider provider, FileSystemPlugin plugin, String schemaName, String storageEngineName,
      DrillFileSystem fileSystem, WorkspaceConfig config,
      List<FormatMatcher> formatMatchers) throws ExecutionSetupException, IOException {
    this.fs = fileSystem;
    this.plugin = plugin;
    this.drillConfig = drillConfig;
    this.config = config;
    this.mapper = drillConfig.getMapper();
    this.fileMatchers = Lists.newArrayList();
    this.dirMatchers = Lists.newArrayList();
    this.storageEngineName = storageEngineName;
    this.schemaName = schemaName;

    // setup cache
    if(storageEngineName == null){
      this.knownViews = null;
//      this.knownPaths = null;
    }else{
      this.knownViews = provider.getPStore(PStoreConfig //
          .newJacksonBuilder(drillConfig.getMapper(), String.class) //
          .name(Joiner.on('.').join("storage.views", storageEngineName, schemaName)) //
          .build());

//      this.knownPaths = provider.getPTable(PTableConfig //
//          .newJacksonBuilder(drillConfig.getMapper(), String.class) //
//          .name(Joiner.on('.').join("storage.cache", storageEngineName, schemaName)) //
//          .build());
    }


    for (FormatMatcher m : formatMatchers) {
      if (m.supportDirectoryReads()) {
        dirMatchers.add(m);
      }
      fileMatchers.add(m);
    }

  }

  private Path getViewPath(String name){
    return new Path(config.getLocation() + '/' + name + ".view.drill");
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

    public boolean createView(View view) throws Exception {
      Path viewPath = getViewPath(view.getName());
      boolean replaced = fs.getUnderlying().exists(viewPath);
      try(DrillOutputStream stream = fs.create(viewPath)){
        mapper.writeValue(stream.getOuputStream(), view);
      }
      if(knownViews != null) knownViews.put(view.getName(), viewPath.toString());
      return replaced;
    }

    public boolean viewExists(String viewName) throws Exception {
      Path viewPath = getViewPath(viewName);
      return fs.getUnderlying().exists(viewPath);
    }

    public void dropView(String viewName) throws IOException {
      fs.getUnderlying().delete(getViewPath(viewName), false);
      if(knownViews != null) knownViews.delete(viewName);
    }

    private ExpandingConcurrentMap<String, DrillTable> tables = new ExpandingConcurrentMap<String, DrillTable>(WorkspaceSchemaFactory.this);

    private UserSession session;

    public WorkspaceSchema(List<String> parentSchemaPath, String name, UserSession session) {
      super(parentSchemaPath, name);
      this.session = session;
    }

    private Set<String> getViews(){
      Set<String> viewSet = Sets.newHashSet();
      if(knownViews != null) {
        for(Map.Entry<String, String> e : knownViews){
          viewSet.add(e.getKey());
        }
      }
      return viewSet;
    }

    @Override
    public Set<String> getTableNames() {
      return Sets.union(tables.keySet(), getViews());
    }

    private View getView(DotDrillFile f) throws Exception{
      assert f.getType() == DotDrillType.VIEW;
      return f.getView(drillConfig);
    }

    @Override
    public Table getTable(String name) {
      // first check existing tables.
      if(tables.alreadyContainsKey(name)) return tables.get(name);

      // then check known views.
//      String path = knownViews.get(name);

      // then look for files that start with this name and end in .drill.
      List<DotDrillFile> files;
      try {
        files = DotDrillUtil.getDotDrills(fs, new Path(config.getLocation()), name, DotDrillType.VIEW);
        for(DotDrillFile f : files){
          switch(f.getType()){
          case VIEW:
            return new DrillViewTable(schemaPath, getView(f));
          }
        }
      } catch (Exception e) {
        logger.warn("Failure while trying to load .drill file.", e);
      }

      return tables.get(name);

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
