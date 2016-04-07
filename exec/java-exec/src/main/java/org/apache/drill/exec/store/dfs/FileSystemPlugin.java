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
import java.util.Map.Entry;
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
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import org.apache.hadoop.fs.FileSystem;

/**
 * A Storage engine associated with a Hadoop FileSystem Implementation. Examples include HDFS, MapRFS, QuantacastFileSystem,
 * LocalFileSystem, as well Apache Drill specific CachedFileSystem, ClassPathFileSystem and LocalSyncableFileSystem.
 * Tables are file names, directories and path patterns. This storage engine delegates to FSFormatEngines but shares
 * references to the FileSystem configuration and path management.
 */
public class FileSystemPlugin extends AbstractStoragePlugin{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemPlugin.class);

  /**
   * Default {@link Configuration} instance. Use this instance to create new copies of {@link Configuration} objects.
   * ex.
   *   Configuration newConf = new Configuration(FileSystemPlugin.DEFAULT_CONFIGURATION);
   *   newConf.set("my.special.property", "my.special.value");
   *
   * It saves the time taken for costly classpath scanning of *-site.xml files for each new {@link Configuration}
   * object created.
   *
   * Note: Don't modify the DEFAULT_CONFIGURATION. Currently there is no way to make the {@link Configuration} object
   * immutable.
   */
  public static final Configuration DEFAULT_CONFIGURATION = new Configuration();

  private final LogicalPlanPersistence lpPersistance;
  private final FileSystemPluginImplementationProvider provider;
  private final FileSystemConfig config;
  private final String storageName;

  private Configuration fsConf;
  private FormatCreator formatCreator;
  private FileSystemSchemaFactory schemaFactory;

  /**
   * Create a default implementation of {@link FileSystemPlugin}
   * @param config
   * @param dContext
   * @param storageName
   * @throws ExecutionSetupException
   */
  public FileSystemPlugin(final FileSystemConfig config, final DrillbitContext dContext, final String storageName)
      throws ExecutionSetupException {
    this(storageName, config, dContext, new FileSystemPluginImplementationProvider(dContext, storageName, config));
  }

  /**
   * Create a {@link FileSystemPlugin} instance with given {@link FileSystemPluginImplementationProvider}.
   * @param name
   * @param config
   * @param dContext
   * @param provider
   * @throws ExecutionSetupException
   */
  public FileSystemPlugin(final String name, final FileSystemConfig config, final DrillbitContext dContext,
      final FileSystemPluginImplementationProvider provider) throws ExecutionSetupException {
    this.storageName = name;
    this.config = config;
    this.lpPersistance = dContext.getLpPersistence();
    this.provider = provider;
  }

  @Override
  public void start() throws IOException {
    try {
      this.fsConf = new Configuration(DEFAULT_CONFIGURATION);
      if (config.getConfig() != null) {
        for (Entry<String, String> prop : config.getConfig().entrySet()) {
          fsConf.set(prop.getKey(), prop.getValue());
        }
      }
      fsConf.set(FileSystem.FS_DEFAULT_NAME_KEY, config.getConnection());

      for(Entry<String, String> prop : provider.getFsProps().entrySet()) {
        fsConf.set(prop.getKey(), prop.getValue());
      }

      this.formatCreator = provider.getFormatCreator(fsConf);
      this.schemaFactory = provider.createSchemaFactory(this, formatCreator, fsConf);
    } catch (IOException | ExecutionSetupException e) {
      throw new IOException("Failure starting up file system plugin.", e);
    }
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
    FileSystemReadEntry fileSystemReadEntry = selection.getWith(lpPersistance, FileSystemReadEntry.class);
    FormatPlugin plugin = formatCreator.getFormatPluginByConfig(fileSystemReadEntry.getFormat());
    if (plugin == null) {
      plugin = formatCreator.newFormatPlugin(fileSystemReadEntry.getFormat());
    }
    return plugin.getGroupScan(userName, this, fileSystemReadEntry.getWorkspace(), fileSystemReadEntry.getSelection(), columns);
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  public FormatPlugin getFormatPlugin(String name) {
    return formatCreator.getFormatPluginByName(name);
  }

  public FormatPlugin getFormatPlugin(FormatPluginConfig config) {
    return formatCreator.getFormatPluginByConfig(config);
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

  /**
   * Get {@link Configuration} for given workspace.
   * @param workspace
   * @return
   */
  public Configuration getFsConf(String workspace) {
    return schemaFactory.getWorkspaceSchemaFactory(workspace).getFsConf();
  }

  public Configuration getFsConf() {
    return fsConf;
  }

  public String getStorageName() {
    return storageName;
  }
}
