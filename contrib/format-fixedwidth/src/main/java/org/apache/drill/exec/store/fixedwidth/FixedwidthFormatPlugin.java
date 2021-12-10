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

package org.apache.drill.exec.store.fixedwidth;

import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileReaderFactory;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileScanBuilder;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.hadoop.conf.Configuration;


public class FixedwidthFormatPlugin extends EasyFormatPlugin<FixedwidthFormatConfig> {

  protected static final String DEFAULT_NAME = "fixedwidth";

  private static class FixedwidthReaderFactory extends FileReaderFactory {

    private final FixedwidthFormatConfig config;
    private final int maxRecords;

    public FixedwidthReaderFactory(FixedwidthFormatConfig config, int maxRecords) {
      this.config = config;
      this.maxRecords = maxRecords;
    }

    @Override
    public ManagedReader<? extends FileSchemaNegotiator> newReader() {
      return new FixedwidthBatchReader(config, maxRecords);
    }
  }

  public FixedwidthFormatPlugin(String name,
                                DrillbitContext context,
                                Configuration fsConf,
                                StoragePluginConfig storageConfig,
                                FixedwidthFormatConfig formatConfig) {
    super(name, easyConfig(fsConf, formatConfig), context, storageConfig, formatConfig);
  }

  private static EasyFormatConfig easyConfig(Configuration fsConf, FixedwidthFormatConfig pluginConfig) {
    return EasyFormatConfig.builder()
      .readable(true)
      .writable(false)
      .blockSplittable(false) // Change to true
      .compressible(true)
      .supportsProjectPushdown(true)
      .extensions(pluginConfig.getExtensions())
      .fsConf(fsConf)
      .defaultName(DEFAULT_NAME)
      .useEnhancedScan(true)
      .supportsLimitPushdown(true)
      .build();
  }

  @Override
  public ManagedReader<? extends FileSchemaNegotiator> newBatchReader(
    EasySubScan scan, OptionManager options) {
    return new FixedwidthBatchReader(getConfig(), scan.getMaxRecords());
  }

  @Override
  protected FileScanBuilder frameworkBuilder(OptionManager options, EasySubScan scan) {
    FileScanBuilder builder = new FileScanBuilder();
    builder.setReaderFactory(new FixedwidthReaderFactory(getConfig(), scan.getMaxRecords()));
    initScanBuilder(builder, scan);
    builder.nullType(Types.optional(TypeProtos.MinorType.VARCHAR));
    return builder;
  }
}
