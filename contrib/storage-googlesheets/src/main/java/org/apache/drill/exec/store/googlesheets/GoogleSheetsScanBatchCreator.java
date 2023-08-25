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
package org.apache.drill.exec.store.googlesheets;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework.ReaderFactory;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework.ScanFrameworkBuilder;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginException;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class GoogleSheetsScanBatchCreator implements BatchCreator<GoogleSheetsSubScan> {

  private static final Logger logger = LoggerFactory.getLogger(GoogleSheetsScanBatchCreator.class);

  @Override
  public CloseableRecordBatch getBatch(ExecutorFragmentContext context,
                                       GoogleSheetsSubScan subScan, List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());

    StoragePluginRegistry registry = context.getDrillbitContext().getStorage();
    GoogleSheetsStoragePlugin plugin;
    try {
      plugin = (GoogleSheetsStoragePlugin) registry.getPlugin(subScan.getScanSpec().getPluginName());
    } catch (PluginException e) {
      throw UserException.internalError(e)
        .message("Unable to locate GoogleSheets storage plugin")
        .build(logger);
    }

    try {
      ScanFrameworkBuilder builder = createBuilder(context.getOptions(), subScan, plugin);
      return builder.buildScanOperator(context, subScan);
    } catch (UserException e) {
      // Rethrow user exceptions directly
      throw e;
    } catch (Throwable e) {
      // Wrap all others
      throw new ExecutionSetupException(e);
    }
  }

  private ScanFrameworkBuilder createBuilder(OptionManager options, GoogleSheetsSubScan subScan, GoogleSheetsStoragePlugin plugin) {
    GoogleSheetsStoragePluginConfig config = subScan.getConfig();
    ScanFrameworkBuilder builder = new ScanFrameworkBuilder();
    builder.projection(subScan.getColumns());
    builder.providedSchema(subScan.getSchema());
    builder.setUserName(subScan.getUserName());

    // Reader
    ReaderFactory readerFactory = new GoogleSheetsReaderFactory(config, subScan, plugin);
    builder.setReaderFactory(readerFactory);
    builder.nullType(Types.optional(MinorType.VARCHAR));
    return builder;
  }

  private static class GoogleSheetsReaderFactory implements ReaderFactory {

    private final GoogleSheetsStoragePluginConfig config;
    private final GoogleSheetsSubScan subScan;
    private final GoogleSheetsStoragePlugin plugin;
    private int count;

    public GoogleSheetsReaderFactory(GoogleSheetsStoragePluginConfig config, GoogleSheetsSubScan subScan, GoogleSheetsStoragePlugin plugin) {
      this.config = config;
      this.subScan = subScan;
      this.plugin = plugin;
    }

    @Override
    public void bind(ManagedScanFramework framework) {
    }

    @Override
    public ManagedReader<SchemaNegotiator> next() {
      // Only a single scan (in a single thread)
      if (count++ == 0) {
        return new GoogleSheetsBatchReader(config, subScan, plugin);
      }
      return null;
    }
  }
}
