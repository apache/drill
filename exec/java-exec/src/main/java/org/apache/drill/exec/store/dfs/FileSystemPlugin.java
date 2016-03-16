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

import static org.apache.drill.exec.store.dfs.FileSystemSchemaFactory.DEFAULT_WS_NAME;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.config.LogicalPlanPersistence;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.ClassPathFileSystem;
import org.apache.drill.exec.store.LocalSyncableFileSystem;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A Storage engine associated with a Hadoop FileSystem Implementation. Examples include HDFS, MapRFS, QuantacastFileSystem,
 * LocalFileSystem, as well Apache Drill specific CachedFileSystem, ClassPathFileSystem and LocalSyncableFileSystem.
 * Tables are file names, directories and path patterns. This storage engine delegates to FSFormatEngines but shares
 * references to the FileSystem configuration and path management.
 */
public class FileSystemPlugin extends AbstractStoragePlugin{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemPlugin.class);

  private final FileSystemSchemaFactory schemaFactory;
  private final FormatCreator formatCreator;
  private final Map<FormatPluginConfig, FormatPlugin> formatPluginsByConfig;
  private final FileSystemConfig config;
  private final Configuration fsConf;
  private final LogicalPlanPersistence lpPersistance;

  public FileSystemPlugin(FileSystemConfig config, DrillbitContext context, String name) throws ExecutionSetupException{
    this.config = config;
    this.lpPersistance = context.getLpPersistence();
    try {

      fsConf = new Configuration();
      if (config.config != null) {
        for (String s : config.config.keySet()) {
          fsConf.set(s, config.config.get(s));
        }
      }
      fsConf.set(FileSystem.FS_DEFAULT_NAME_KEY, config.connection);
      fsConf.set("fs.classpath.impl", ClassPathFileSystem.class.getName());
      fsConf.set("fs.drill-local.impl", LocalSyncableFileSystem.class.getName());

      formatCreator = newFormatCreator(config, context, fsConf);
      List<FormatMatcher> matchers = Lists.newArrayList();
      formatPluginsByConfig = Maps.newHashMap();
      for (FormatPlugin p : formatCreator.getConfiguredFormatPlugins()) {
        matchers.add(p.getMatcher());
        formatPluginsByConfig.put(p.getConfig(), p);
      }

      final boolean noWorkspace = config.workspaces == null || config.workspaces.isEmpty();
      List<WorkspaceSchemaFactory> factories = Lists.newArrayList();
      if (!noWorkspace) {
        for (Map.Entry<String, WorkspaceConfig> space : config.workspaces.entrySet()) {
          factories.add(new WorkspaceSchemaFactory(this, space.getKey(), name, space.getValue(), matchers, context.getLpPersistence(), context.getClasspathScan()));
        }
      }

      // if the "default" workspace is not given add one.
      if (noWorkspace || !config.workspaces.containsKey(DEFAULT_WS_NAME)) {
        factories.add(new WorkspaceSchemaFactory(this, DEFAULT_WS_NAME, name, WorkspaceConfig.DEFAULT, matchers, context.getLpPersistence(), context.getClasspathScan()));
      }

      this.schemaFactory = new FileSystemSchemaFactory(name, factories);
    } catch (IOException e) {
      throw new ExecutionSetupException("Failure setting up file system plugin.", e);
    }
  }

  /**
   * Creates a new FormatCreator instance.
   *
   * To be used by subclasses to return custom formats if required.
   * Note that this method is called by the constructor, which fields may not be initialized yet.
   *
   * @param config the plugin configuration
   * @param context the drillbit context
   * @return a new FormatCreator instance
   */
  protected FormatCreator newFormatCreator(FileSystemConfig config, DrillbitContext context, Configuration fsConf) {
    return new FormatCreator(context, fsConf, config, context.getClasspathScan());
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public StoragePluginConfig getConfig() {
    return config;
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> columns)
      throws IOException {
    FormatSelection formatSelection = selection.getWith(lpPersistance, FormatSelection.class);
    FormatPlugin plugin;
    if (formatSelection.getFormat() instanceof NamedFormatPluginConfig) {
      plugin = formatCreator.getFormatPluginByName( ((NamedFormatPluginConfig) formatSelection.getFormat()).name);
    } else {
      plugin = formatPluginsByConfig.get(formatSelection.getFormat());
    }
    if (plugin == null) {
      plugin = formatCreator.newFormatPlugin(formatSelection.getFormat());
    }
    return plugin.getGroupScan(userName, formatSelection.getSelection(), columns);
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  public FormatPlugin getFormatPlugin(String name) {
    return formatCreator.getFormatPluginByName(name);
  }

  public FormatPlugin getFormatPlugin(FormatPluginConfig config) {
    if (config instanceof NamedFormatPluginConfig) {
      return formatCreator.getFormatPluginByName(((NamedFormatPluginConfig) config).name);
    } else {
      return formatPluginsByConfig.get(config);
    }
  }

  @Override
  public Set<StoragePluginOptimizerRule> getPhysicalOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
    Builder<StoragePluginOptimizerRule> setBuilder = ImmutableSet.builder();
    for(FormatPlugin plugin : formatCreator.getConfiguredFormatPlugins()){
      Set<StoragePluginOptimizerRule> rules = plugin.getOptimizerRules();
      if(rules != null && rules.size() > 0){
        setBuilder.addAll(rules);
      }
    }
    return setBuilder.build();
  }

  public Configuration getFsConf() {
    return fsConf;
  }
}
