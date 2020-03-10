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
package org.apache.drill.exec.store.avro;

import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.hadoop.conf.Configuration;

/**
 * Format plugin for Avro data files.
 */
public class AvroFormatPlugin extends EasyFormatPlugin<AvroFormatConfig> {

  public static final String DEFAULT_NAME = "avro";

  public AvroFormatPlugin(String name,
                          DrillbitContext context,
                          Configuration fsConf,
                          StoragePluginConfig storageConfig,
                          AvroFormatConfig formatConfig) {
    super(name, easyConfig(fsConf, formatConfig), context, storageConfig, formatConfig);
  }

  private static EasyFormatConfig easyConfig(Configuration fsConf, AvroFormatConfig formatConfig) {
    EasyFormatConfig config = new EasyFormatConfig();
    config.readable = true;
    config.writable = false;
    config.blockSplittable = true;
    config.compressible = false;
    config.supportsProjectPushdown = true;
    config.extensions = formatConfig.extensions;
    config.fsConf = fsConf;
    config.defaultName = DEFAULT_NAME;
    config.readerOperatorType = CoreOperatorType.AVRO_SUB_SCAN_VALUE;
    config.useEnhancedScan = true;
    return config;
  }

  @Override
  protected FileScanFramework.FileScanBuilder frameworkBuilder(OptionManager options, EasySubScan scan) {
    FileScanFramework.FileScanBuilder builder = new FileScanFramework.FileScanBuilder();
    builder.setReaderFactory(new AvroReaderFactory());
    initScanBuilder(builder, scan);
    builder.nullType(Types.optional(TypeProtos.MinorType.VARCHAR));
    return builder;
  }

  private static class AvroReaderFactory extends FileScanFramework.FileReaderFactory {

    @Override
    public ManagedReader<? extends FileScanFramework.FileSchemaNegotiator> newReader() {
      return new AvroBatchReader();
    }
  }
}
