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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.drill.common.config.LogicalPlanPersistence;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * {@link SchemaFactory} implementation for creating workspaces in {@link FileSystemPlugin}.
 */
public class WorkspaceSchemaFactory {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WorkspaceSchemaFactory.class);

  private final List<FormatMatcher> fileMatchers;
  private final List<FormatMatcher> dropFileMatchers;
  private final List<FormatMatcher> dirMatchers;

  private final WorkspaceConfig config;
  private final Configuration fsConf;
  private final String storageEngineName;
  private final String schemaName;
  private final FileSystemPlugin plugin;
  private final LogicalPlanPersistence logicalPlanPersistence;
  private final Path wsPath;

  private final FormatPluginOptionExtractor optionExtractor;

  public WorkspaceSchemaFactory(
      FileSystemPlugin plugin,
      Configuration fsConf,
      String schemaName,
      String storageEngineName,
      WorkspaceConfig config,
      List<FormatMatcher> formatMatchers,
      LogicalPlanPersistence logicalPlanPersistence,
      ScanResult scanResult) throws ExecutionSetupException, IOException {
    this.logicalPlanPersistence = logicalPlanPersistence;
    this.fsConf = fsConf;
    this.plugin = plugin;
    this.config = config;
    this.fileMatchers = Lists.newArrayList();
    this.dirMatchers = Lists.newArrayList();
    this.storageEngineName = storageEngineName;
    this.schemaName = schemaName;
    this.wsPath = new Path(config.getLocation());
    this.optionExtractor = new FormatPluginOptionExtractor(scanResult);

    for (FormatMatcher m : formatMatchers) {
      if (m.supportDirectoryReads()) {
        dirMatchers.add(m);
      }
      fileMatchers.add(m);
    }

    // NOTE: Add fallback format matcher if given in the configuration. Make sure fileMatchers is an order-preserving list.
    final String defaultInputFormat = config.getDefaultInputFormat();
    if (!Strings.isNullOrEmpty(defaultInputFormat)) {
      final FormatPlugin formatPlugin = plugin.getFormatPlugin(defaultInputFormat);
      if (formatPlugin == null) {
        final String message = String.format("Unable to find default input format[%s] for workspace[%s.%s]",
            defaultInputFormat, storageEngineName, schemaName);
        throw new ExecutionSetupException(message);
      }
      final FormatMatcher fallbackMatcher = new BasicFormatMatcher(formatPlugin,
          ImmutableList.of(Pattern.compile(".*")), ImmutableList.<MagicString>of());
      fileMatchers.add(fallbackMatcher);
      dropFileMatchers = fileMatchers.subList(0, fileMatchers.size() - 1);
    } else {
      dropFileMatchers = fileMatchers.subList(0, fileMatchers.size());
    }
  }

  /**
   * Checks whether the given user has permission to list files/directories under the workspace directory.
   *
   * @param userName User who is trying to access the workspace.
   * @return True if the user has access. False otherwise.
   */
  public boolean accessible(final String userName) throws IOException {
    final FileSystem fs = ImpersonationUtil.createFileSystem(userName, fsConf);
    try {
      // We have to rely on the listStatus as a FileSystem can have complicated controls such as regular unix style
      // permissions, Access Control Lists (ACLs) or Access Control Expressions (ACE). Hadoop 2.7 version of FileSystem
      // has a limited private API (FileSystem.access) to check the permissions directly
      // (see https://issues.apache.org/jira/browse/HDFS-6570). Drill currently relies on Hadoop 2.5.0 version of
      // FileClient. TODO: Update this when DRILL-3749 is fixed.
      fs.listStatus(wsPath);
    } catch (final UnsupportedOperationException e) {
      logger.trace("The filesystem for this workspace does not support this operation.", e);
    } catch (final FileNotFoundException | AccessControlException e) {
      return false;
    }

    return true;
  }

  protected Path getWsPath() {
    return wsPath;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public WorkspaceConfig getWorkspaceConfig() {
    return config;
  }

  public LogicalPlanPersistence getLogicalPlanPersistence() {
    return logicalPlanPersistence;
  }

  public FormatPluginOptionExtractor getOptionExtractor() {
    return optionExtractor;
  }

  public FileSystemPlugin getFileSystemPlugin() {
    return plugin;
  }

  public Configuration getFsConf() {
    return fsConf;
  }

  public List<FormatMatcher> getFileMatchers() {
    return fileMatchers;
  }

  public List<FormatMatcher> getDropFileMatchers() {
    return dropFileMatchers;
  }

  public List<FormatMatcher> getDirMatchers() {
    return dirMatchers;
  }

  public WorkspaceSchema createSchema(List<String> parentSchemaPath, SchemaConfig schemaConfig) throws IOException {
    return new WorkspaceSchema(this, parentSchemaPath, schemaConfig);
  }
}
