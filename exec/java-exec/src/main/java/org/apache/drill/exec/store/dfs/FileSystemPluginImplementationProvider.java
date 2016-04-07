/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.dfs;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.ClassPathFileSystem;
import org.apache.drill.exec.store.LocalSyncableFileSystem;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.drill.exec.store.dfs.FileSystemSchemaFactory.DEFAULT_WS_NAME;

/**
 * Provides needed components by the {@link FileSystemPlugin}. This can be overridden to supply customized components
 * (such as custom schema factory) to {@link FileSystemPlugin}.
 */
public class FileSystemPluginImplementationProvider {

  protected final FileSystemConfig fsConfig;
  protected final String fsPluginName;
  protected final DrillbitContext dContext;

  /**
   * Instantiate an object
   * @param dContext {@link DrillbitContext} instance.
   * @param fsPluginName Name of the FileSystemPlugin storage
   * @param fsConfig FileSystemPlugin configuration
   */
  public FileSystemPluginImplementationProvider(
      final DrillbitContext dContext,
      final String fsPluginName,
      final FileSystemConfig fsConfig) {
    this.dContext = dContext;
    this.fsConfig = fsConfig;
    this.fsPluginName = fsPluginName;
  }

  /**
   * @return Return any properties needed for {@link Configuration} object that are not set in
   * {@link FileSystemPlugin}'s configuration.
   */
  public Map<String, String> getFsProps() {
    return ImmutableMap.of(
        "fs.classpath.impl", ClassPathFileSystem.class.getName(),
        "fs.drill-local.impl", LocalSyncableFileSystem.class.getName()
    );
  }

  /**
   * @return Create and return {@link FormatCreator} based on the storage plugin configuration.
   */
  public FormatCreator getFormatCreator(final Configuration fsConf) {
    return new FormatCreator(dContext, fsConf, fsConfig, dContext.getClasspathScan());
  }

  /**
   * Create and return list of {@link WorkspaceSchemaFactory}'s based on the storage plugin configuration.
   *
   * @param fsPlugin Reference to {@link FileSystemPlugin} which these workspaces are part of.
   * @param formatCreator
   * @param fsConf {@link Configuration} of the FileSystemPlugin
   * @return List of {@link WorkspaceSchemaFactory}'s
   * @throws IOException
   * @throws ExecutionSetupException
   */
  protected Map<String, WorkspaceSchemaFactory> createWorkspaceSchemaFactories(final FileSystemPlugin fsPlugin,
      final FormatCreator formatCreator, final Configuration fsConf) throws IOException, ExecutionSetupException {
    final Map<String, WorkspaceConfig> workspaces = fsConfig.getWorkspaces();
    final Map<String, WorkspaceSchemaFactory> factories = Maps.newHashMap();
    if (workspaces != null) {
      for (Map.Entry<String, WorkspaceConfig> space : workspaces.entrySet()) {
        factories.put(space.getKey(),
            createWorkspaceSchemaFactory(fsPlugin, formatCreator, fsConf, space.getKey(), space.getValue()));
      }
    }

    return factories;
  }

  /**
   * Create and return a {@link WorkspaceSchemaFactory} with given name and {@link WorkspaceConfig}.
   *
   * @param fsPlugin Reference to {@link FileSystemPlugin} which this workspace is part of.
   * @param formatCreator
   * @param fsConf {@link Configuration} of the FileSystemPlugin
   * @param wsName Name of the workspace schema.
   * @param wsConfig Workspace configuration.
   * @return
   * @throws IOException
   * @throws ExecutionSetupException
   */
  protected WorkspaceSchemaFactory createWorkspaceSchemaFactory(final FileSystemPlugin fsPlugin,
      final FormatCreator formatCreator, final Configuration fsConf, final String wsName,
      final WorkspaceConfig wsConfig) throws IOException, ExecutionSetupException {
    final Configuration wsFsConf;
    if (wsConfig.getConfig() == null || wsConfig.getConfig().size() == 0) {
      wsFsConf = fsConf;
    } else {
      wsFsConf = new Configuration(fsConf);
      for(Entry<String, String> entry : wsConfig.getConfig().entrySet()) {
        wsFsConf.set(entry.getKey(), entry.getValue());
      }
    }

    return new WorkspaceSchemaFactory(fsPlugin, wsFsConf, wsName, fsPluginName, wsConfig,
        formatCreator.getFormatMatchers(), dContext.getLpPersistence(), dContext.getClasspathScan());
  }

  /**
   * Create and return a {@link FileSystemSchemaFactory} based on the storage plugin configuration. If there are
   * no workspaces defined in the storage plugin configuration or no workspace with name "default,
   * a default one is created with "/" as the workspace schema location.
   *
   * @param fsPlugin Reference to {@link FileSystemPlugin}
   * @param formatCreator
   * @param fsConf {@link Configuration} of the FileSystemPlugin
   *
   * @return
   * @throws IOException
   * @throws ExecutionSetupException
   */
  public FileSystemSchemaFactory createSchemaFactory(final FileSystemPlugin fsPlugin, final FormatCreator formatCreator,
      final Configuration fsConf) throws IOException, ExecutionSetupException {

    final Map<String, WorkspaceSchemaFactory> wsFactories =
        createWorkspaceSchemaFactories(fsPlugin, formatCreator, fsConf);

    // if the "default" workspace is not given add one.
    if (wsFactories.size() == 0 || !wsFactories.containsKey(DEFAULT_WS_NAME)) {
      wsFactories.put(DEFAULT_WS_NAME,
          createWorkspaceSchemaFactory(fsPlugin, formatCreator, fsConf, DEFAULT_WS_NAME, WorkspaceConfig.DEFAULT));
    }

    return new FileSystemSchemaFactory(fsPluginName, wsFactories);
  }
}
