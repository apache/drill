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
package org.apache.drill.exec.store.dfs.easy;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileReaderFactory;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileScanBuilder;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create the plugin-specific framework that manages the scan. The framework
 * creates batch readers one by one for each file or block. It defines semantic
 * rules for projection. It handles "early" or "late" schema readers. A typical
 * framework builds on standardized frameworks for files in general or text
 * files in particular.
 * <p>
 * This is for "version 1" of EVF. Newer code should use "version 2."
 *
 * @return the scan framework which orchestrates the scan operation across
 * potentially many files
 * @throws ExecutionSetupException for all setup failures
 */
class EvfV1ScanBuilder {
  private static final Logger logger = LoggerFactory.getLogger(EvfV1ScanBuilder.class);

  /**
   * Builds the readers for the V1 row-set based scan operator.
   */
  private static class EasyReaderFactory extends FileReaderFactory {

    private final EasyFormatPlugin<? extends FormatPluginConfig> plugin;
    private final EasySubScan scan;
    private final FragmentContext context;

    public EasyReaderFactory(EasyFormatPlugin<? extends FormatPluginConfig> plugin,
        EasySubScan scan, FragmentContext context) {
      this.plugin = plugin;
      this.scan = scan;
      this.context = context;
    }

    @Override
    public ManagedReader<? extends FileSchemaNegotiator> newReader() {
      try {
        return plugin.newBatchReader(scan, context.getOptions());
      } catch (ExecutionSetupException e) {
        throw UserException.validationError(e)
          .addContext("Reason", "Failed to create a batch reader")
          .addContext(errorContext())
          .build(logger);
      }
    }
  }

  private final FragmentContext context;
  private final EasySubScan scan;
  private final EasyFormatPlugin<? extends FormatPluginConfig> plugin;

  public EvfV1ScanBuilder(FragmentContext context, EasySubScan scan,
      EasyFormatPlugin<? extends FormatPluginConfig> plugin) {
    this.context = context;
    this.scan = scan;
    this.plugin = plugin;
  }

  /**
   * Revised scanner based on the revised {@link org.apache.drill.exec.physical.resultSet.ResultSetLoader}
   * and {@link org.apache.drill.exec.physical.impl.scan.RowBatchReader} classes.
   * Handles most projection tasks automatically. Able to limit
   * vector and batch sizes. Use this for new format plugins.
   */
  public CloseableRecordBatch build() throws ExecutionSetupException {
    final FileScanBuilder builder = plugin.frameworkBuilder(context.getOptions(), scan);

    // Add batch reader, if none specified

    if (builder.readerFactory() == null) {
      builder.setReaderFactory(new EasyReaderFactory(plugin, scan, context));
    }
    return builder.buildScanOperator(context, scan);
  }

  /**
   * Initialize the scan framework builder with standard options.
   * Call this from the plugin-specific
   * {@link #frameworkBuilder(OptionManager, EasySubScan)} method.
   * The plugin can then customize/revise options as needed.
   *
   * @param builder the scan framework builder you create in the
   * {@link #frameworkBuilder(OptionManager, EasySubScan)} method
   * @param scan the physical scan operator definition passed to
   * the {@link #frameworkBuilder(OptionManager, EasySubScan)} method
   */
  protected static void initScanBuilder(EasyFormatPlugin<? extends FormatPluginConfig> plugin,
      FileScanBuilder builder, EasySubScan scan) {
    builder.projection(scan.getColumns());
    builder.setUserName(scan.getUserName());

    // Pass along the output schema, if any
    builder.providedSchema(scan.getSchema());

    // Pass along file path information
    builder.setFileSystemConfig(plugin.getFsConf());
    builder.setFiles(scan.getWorkUnits());
    final Path selectionRoot = scan.getSelectionRoot();
    if (selectionRoot != null) {
      builder.implicitColumnOptions().setSelectionRoot(selectionRoot);
      builder.implicitColumnOptions().setPartitionDepth(scan.getPartitionDepth());
    }

    // Additional error context to identify this plugin
    builder.errorContext(
        currentBuilder -> currentBuilder
            .addContext("Format plugin", plugin.easyConfig().getDefaultName())
            .addContext("Format plugin", plugin.getClass().getSimpleName())
            .addContext("Plugin config name", plugin.getName()));
  }
}
