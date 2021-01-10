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
package org.apache.drill.exec.store.esri;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;

import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileReaderFactory;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.drill.exec.store.esri.ShpBatchReader.ShpReaderConfig;
import org.apache.hadoop.conf.Configuration;

public class ShpFormatPlugin extends EasyFormatPlugin<ShpFormatConfig> {

  public static final String PLUGIN_NAME = "shp";

  public static class ShpReaderFactory extends FileReaderFactory {
    private final ShpReaderConfig readerConfig;

    private final int maxRecords;

    public ShpReaderFactory(ShpReaderConfig config, int maxRecords) {
      readerConfig = config;
      this.maxRecords = maxRecords;
    }

    @Override
    public ManagedReader<? extends FileScanFramework.FileSchemaNegotiator> newReader() {
      return new ShpBatchReader(readerConfig, maxRecords);
    }
  }

  public ShpFormatPlugin(String name, DrillbitContext context, Configuration fsConf, StoragePluginConfig storageConfig, ShpFormatConfig formatConfig) {
    super(name, easyConfig(fsConf, formatConfig), context, storageConfig, formatConfig);
  }

  @Override
  public ManagedReader<? extends FileSchemaNegotiator> newBatchReader(EasySubScan scan, OptionManager options) throws ExecutionSetupException {
    return new ShpBatchReader(formatConfig.getReaderConfig(this), scan.getMaxRecords());
  }

  @Override
  protected FileScanFramework.FileScanBuilder frameworkBuilder(OptionManager options, EasySubScan scan) {
    FileScanFramework.FileScanBuilder builder = new FileScanFramework.FileScanBuilder();
    builder.setReaderFactory(new ShpReaderFactory(new ShpReaderConfig(this), scan.getMaxRecords()));
    initScanBuilder(builder, scan);
    builder.nullType(Types.optional(TypeProtos.MinorType.VARCHAR));
    return builder;
  }

  private static EasyFormatConfig easyConfig(Configuration fsConf, ShpFormatConfig pluginConfig) {
    return EasyFormatConfig.builder()
        .readable(true)
        .writable(false)
        .blockSplittable(false)
        .compressible(false)
        .supportsProjectPushdown(true)
        .extensions(pluginConfig.getExtensions())
        .fsConf(fsConf)
        .defaultName(PLUGIN_NAME)
        .useEnhancedScan(true)
        .supportsLimitPushdown(true)
        .build();
  }
}
