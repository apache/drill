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
package org.apache.drill.exec.store.image;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileReaderFactory;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileScanBuilder;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin.ScanFrameworkVersion;
import org.apache.hadoop.conf.Configuration;

public class ImageFormatPlugin extends EasyFormatPlugin<ImageFormatConfig> {

  public ImageFormatPlugin(String name,
                           DrillbitContext context,
                           Configuration fsConf,
                           StoragePluginConfig storageConfig,
                           ImageFormatConfig formatConfig) {
    super(name, easyConfig(fsConf, formatConfig), context, storageConfig, formatConfig);
  }

  private static EasyFormatConfig easyConfig(Configuration fsConf, ImageFormatConfig pluginConfig) {
    return EasyFormatConfig.builder()
        .readable(true)
        .writable(false)
        .blockSplittable(false)
        .compressible(true)
        .extensions(pluginConfig.getExtensions())
        .fsConf(fsConf)
        .scanVersion(ScanFrameworkVersion.EVF_V1)
        .supportsLimitPushdown(true)
        .supportsProjectPushdown(true)
        .defaultName(ImageFormatConfig.NAME)
        .build();
  }

  private static class ImageReaderFactory extends FileReaderFactory {

    private final ImageFormatConfig config;
    private final EasySubScan scan;

    public ImageReaderFactory(ImageFormatConfig config, EasySubScan scan) {
      this.config = config;
      this.scan = scan;
    }

    @Override
    public ManagedReader<? extends FileSchemaNegotiator> newReader() {
      return new ImageBatchReader(config, scan);
    }
  }

  @Override
  public ManagedReader<? extends FileSchemaNegotiator> newBatchReader(EasySubScan scan, OptionSet options)
      throws ExecutionSetupException {
    return new ImageBatchReader(formatConfig, scan);
  }

  @Override
  protected FileScanBuilder frameworkBuilder(EasySubScan scan, OptionSet options)
      throws ExecutionSetupException {
    FileScanBuilder builder = new FileScanBuilder();
    builder.setReaderFactory(new ImageReaderFactory(formatConfig, scan));

    initScanBuilder(builder, scan);
    builder.nullType(Types.optional(MinorType.VARCHAR));
    return builder;
  }
}
