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
package org.apache.drill.exec.store.pcap;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileReaderFactory;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileScanBuilder;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.hadoop.conf.Configuration;
import org.apache.drill.exec.store.pcap.PcapBatchReader.PcapReaderConfig;

public class PcapFormatPlugin extends EasyFormatPlugin<PcapFormatConfig> {

  public static final String PLUGIN_NAME = "pcap";

  private static class PcapReaderFactory extends FileReaderFactory {

    private final PcapReaderConfig readerConfig;

    public PcapReaderFactory(PcapReaderConfig config) {
      readerConfig = config;
    }

    @Override
    public ManagedReader<? extends FileSchemaNegotiator> newReader() {
      return new PcapBatchReader(readerConfig);
    }
  }

  public PcapFormatPlugin(String name, DrillbitContext context,
                           Configuration fsConf, StoragePluginConfig storageConfig,
                           PcapFormatConfig formatConfig) {
    super(name, easyConfig(fsConf, formatConfig), context, storageConfig, formatConfig);
  }

  private static EasyFormatConfig easyConfig(Configuration fsConf, PcapFormatConfig pluginConfig) {
    EasyFormatConfig config = new EasyFormatConfig();
    config.readable = true;
    config.writable = false;
    config.blockSplittable = false;
    config.compressible = true;
    config.supportsProjectPushdown = true;
    config.extensions = pluginConfig.getExtensions();
    config.fsConf = fsConf;
    config.defaultName = PLUGIN_NAME;
    config.readerOperatorType = UserBitShared.CoreOperatorType.PCAP_SUB_SCAN_VALUE;
    config.useEnhancedScan = true;
    return config;
  }

  @Override
  public ManagedReader<? extends FileSchemaNegotiator> newBatchReader(EasySubScan scan, OptionManager options) {
    return new PcapBatchReader(new PcapReaderConfig(this));
  }

  @Override
  protected FileScanBuilder frameworkBuilder(OptionManager options, EasySubScan scan) {
    FileScanBuilder builder = new FileScanBuilder();
    builder.setReaderFactory(new PcapReaderFactory(new PcapReaderConfig(this)));
    initScanBuilder(builder, scan);
    builder.setNullType(Types.optional(TypeProtos.MinorType.VARCHAR));
    return builder;
  }
}
