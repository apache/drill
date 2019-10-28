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
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.drill.exec.store.esri.ShpBatchReader.ShpReaderConfig;
import org.apache.hadoop.conf.Configuration;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShpFormatPlugin extends EasyFormatPlugin<ShpFormatConfig> {

  private static final Logger logger = LoggerFactory.getLogger(ShpFormatPlugin.class);

  public static final String PLUGIN_NAME = "shp";

  public static class ShpReaderFactory extends FileReaderFactory {
    private final ShpReaderConfig readerConfig;

    public ShpReaderFactory(ShpReaderConfig config) {
      readerConfig = config;
    }

    @Override
    public ManagedReader<? extends FileScanFramework.FileSchemaNegotiator> newReader() {
      return new ShpBatchReader(readerConfig);
    }
  }

  public ShpFormatPlugin(String name, DrillbitContext context, Configuration fsConf, StoragePluginConfig storageConfig, ShpFormatConfig formatConfig) {
    super(name, easyConfig(fsConf, formatConfig), context, storageConfig, formatConfig);
  }

  @Override
  public ManagedReader<? extends FileSchemaNegotiator> newBatchReader(EasySubScan scan, OptionManager options) throws ExecutionSetupException {
    return new ShpBatchReader(formatConfig.getReaderConfig(this));
  }

  @Override
  protected FileScanFramework.FileScanBuilder frameworkBuilder(OptionManager options, EasySubScan scan) {
    FileScanFramework.FileScanBuilder builder = new FileScanFramework.FileScanBuilder();
    builder.setReaderFactory(new ShpReaderFactory(new ShpReaderConfig(this)));
    initScanBuilder(builder, scan);
    builder.setNullType(Types.optional(TypeProtos.MinorType.VARCHAR));
    return builder;
  }

  private static EasyFormatConfig easyConfig(Configuration fsConf, ShpFormatConfig pluginConfig) {
    EasyFormatConfig config = new EasyFormatConfig();
    config.readable = true;
    config.writable = false;
    config.blockSplittable = false;
    config.compressible = false;
    config.supportsProjectPushdown = true;
    config.extensions = Lists.newArrayList(pluginConfig.getExtensions());
    config.fsConf = fsConf;
    config.defaultName = PLUGIN_NAME;
    config.readerOperatorType = CoreOperatorType.SHP_SUB_SCAN_VALUE;
    config.useEnhancedScan = true;
    return config;
  }
}
