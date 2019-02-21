/*
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
package org.apache.drill.exec.util;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;

import org.apache.drill.exec.store.easy.sequencefile.SequenceFileFormatConfig;
import org.apache.drill.exec.store.easy.text.TextFormatPlugin;

/**
 * This class contains utility methods to speed up tests. Some of the production code currently calls this method
 * when the production code is executed as part of the test runs. That's the reason why this code has to be in
 * production module.
 */
public class StoragePluginTestUtils {
  public static final String CP_PLUGIN_NAME = "cp";
  public static final String DFS_PLUGIN_NAME = "dfs";

  public static final String TMP_SCHEMA = "tmp";
  public static final String ROOT_SCHEMA = "root";

  public static final String DFS_TMP_SCHEMA = DFS_PLUGIN_NAME + "." + TMP_SCHEMA;
  public static final String DFS_DEFAULT_SCHEMA = DFS_PLUGIN_NAME + "." + SchemaFactory.DEFAULT_WS_NAME;
  public static final String DFS_ROOT_SCHEMA = DFS_PLUGIN_NAME + "." + ROOT_SCHEMA;

  public static final String UNIT_TEST_PROP_PREFIX = "drillJDBCUnitTests";
  public static final String UNIT_TEST_DFS_TMP_PROP = UNIT_TEST_PROP_PREFIX + "." + DFS_TMP_SCHEMA;
  public static final String UNIT_TEST_DFS_DEFAULT_PROP = UNIT_TEST_PROP_PREFIX + "." + DFS_DEFAULT_SCHEMA;
  public static final String UNIT_TEST_DFS_ROOT_PROP = UNIT_TEST_PROP_PREFIX + "." + DFS_ROOT_SCHEMA;

  /**
   * Update the workspace locations for a plugin.
   *
   * @param pluginName The plugin to update.
   * @param pluginRegistry A plugin registry.
   * @param tmpDirPath The directory to use.
   */
  public static void updateSchemaLocation(final String pluginName,
                                          final StoragePluginRegistry pluginRegistry,
                                          final File tmpDirPath,
                                          String... schemas) throws ExecutionSetupException {
    @SuppressWarnings("resource")
    final FileSystemPlugin plugin = (FileSystemPlugin) pluginRegistry.getPlugin(pluginName);
    final FileSystemConfig pluginConfig = (FileSystemConfig) plugin.getConfig();

    Map<String, WorkspaceConfig> newWorkspaces = new HashMap<>();
    Optional.ofNullable(pluginConfig.getWorkspaces())
      .ifPresent(newWorkspaces::putAll);

    if (schemas.length == 0) {
      schemas = new String[]{TMP_SCHEMA};
    }

    for (String schema : schemas) {
      WorkspaceConfig workspaceConfig = newWorkspaces.get(schema);
      String inputFormat = workspaceConfig == null ? null : workspaceConfig.getDefaultInputFormat();
      WorkspaceConfig newWorkspaceConfig = new WorkspaceConfig(tmpDirPath.getAbsolutePath(), true, inputFormat, false);
      newWorkspaces.put(schema, newWorkspaceConfig);
    }

    FileSystemConfig newPluginConfig = new FileSystemConfig(
        pluginConfig.getConnection(),
        pluginConfig.getConfig(),
        newWorkspaces,
        pluginConfig.getFormats());
    newPluginConfig.setEnabled(pluginConfig.isEnabled());
    pluginRegistry.createOrUpdate(pluginName, newPluginConfig, true);
  }

  public static void configureFormatPlugins(StoragePluginRegistry pluginRegistry) throws ExecutionSetupException {
    configureFormatPlugins(pluginRegistry, CP_PLUGIN_NAME);
    configureFormatPlugins(pluginRegistry, DFS_PLUGIN_NAME);
  }

  public static void configureFormatPlugins(StoragePluginRegistry pluginRegistry, String storagePlugin) throws ExecutionSetupException {
    FileSystemPlugin fileSystemPlugin = (FileSystemPlugin) pluginRegistry.getPlugin(storagePlugin);
    FileSystemConfig fileSystemConfig = (FileSystemConfig) fileSystemPlugin.getConfig();

    Map<String, FormatPluginConfig> newFormats = new HashMap<>();
    Optional.ofNullable(fileSystemConfig.getFormats())
      .ifPresent(newFormats::putAll);

    TextFormatPlugin.TextFormatConfig textConfig = new TextFormatPlugin.TextFormatConfig();
    textConfig.extensions = ImmutableList.of("txt");
    textConfig.fieldDelimiter = '\u0000';
    newFormats.put("txt", textConfig);

    TextFormatPlugin.TextFormatConfig ssvConfig = new TextFormatPlugin.TextFormatConfig();
    ssvConfig.extensions = ImmutableList.of("ssv");
    ssvConfig.fieldDelimiter = ' ';
    newFormats.put("ssv", ssvConfig);

    TextFormatPlugin.TextFormatConfig psvConfig = new TextFormatPlugin.TextFormatConfig();
    psvConfig.extensions = ImmutableList.of("tbl");
    psvConfig.fieldDelimiter = '|';
    newFormats.put("psv", psvConfig);

    SequenceFileFormatConfig seqConfig = new SequenceFileFormatConfig();
    seqConfig.extensions = ImmutableList.of("seq");
    newFormats.put("sequencefile", seqConfig);

    TextFormatPlugin.TextFormatConfig csvhtestConfig = new TextFormatPlugin.TextFormatConfig();
    csvhtestConfig.extensions = ImmutableList.of("csvh-test");
    csvhtestConfig.fieldDelimiter = ',';
    csvhtestConfig.extractHeader = true;
    csvhtestConfig.skipFirstLine = true;
    newFormats.put("csvh-test", csvhtestConfig);

    FileSystemConfig newFileSystemConfig = new FileSystemConfig(
        fileSystemConfig.getConnection(),
        fileSystemConfig.getConfig(),
        fileSystemConfig.getWorkspaces(),
        newFormats);
    newFileSystemConfig.setEnabled(fileSystemConfig.isEnabled());

    pluginRegistry.createOrUpdate(storagePlugin, newFileSystemConfig, true);
  }
}
