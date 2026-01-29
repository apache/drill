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
package org.apache.drill.exec.store.paimon.format;

import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.FormatLocationTransformer;
import org.apache.drill.exec.store.dfs.FormatMatcher;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.store.plan.rel.PluginDrillTable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class PaimonFormatMatcher extends FormatMatcher {
  private static final String SNAPSHOT_DIR_NAME = "snapshot";
  private static final String SCHEMA_DIR_NAME = "schema";

  private final PaimonFormatPlugin formatPlugin;

  public PaimonFormatMatcher(PaimonFormatPlugin formatPlugin) {
    this.formatPlugin = formatPlugin;
  }

  @Override
  public boolean supportDirectoryReads() {
    return true;
  }

  @Override
  public DrillTable isReadable(DrillFileSystem fs, FileSelection selection, FileSystemPlugin fsPlugin,
    String storageEngineName, SchemaConfig schemaConfig) throws IOException {
    Path selectionRoot = selection.getSelectionRoot();
    Path snapshotDir = new Path(selectionRoot, SNAPSHOT_DIR_NAME);
    Path schemaDir = new Path(selectionRoot, SCHEMA_DIR_NAME);
    if (fs.isDirectory(selectionRoot)
      && fs.exists(snapshotDir) && fs.isDirectory(snapshotDir)
      && fs.exists(schemaDir) && fs.isDirectory(schemaDir)) {
      FormatSelection formatSelection = new FormatSelection(formatPlugin.getConfig(), selection);
      return new PluginDrillTable(fsPlugin, storageEngineName, schemaConfig.getUserName(),
        formatSelection, formatPlugin.getConvention());
    }
    return null;
  }

  @Override
  public boolean isFileReadable(DrillFileSystem fs, FileStatus status) {
    return false;
  }

  @Override
  public FormatPlugin getFormatPlugin() {
    return formatPlugin;
  }

  @Override
  public int priority() {
    return HIGH_PRIORITY;
  }

  @Override
  public FormatLocationTransformer getFormatLocationTransformer() {
    return PaimonFormatLocationTransformer.INSTANCE;
  }
}
