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

package org.apache.drill.exec.store.daffodil;


import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileReaderFactory;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileScanLifecycleBuilder;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileSchemaNegotiator;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.hadoop.conf.Configuration;


public class DaffodilFormatPlugin extends EasyFormatPlugin<DaffodilFormatConfig> {

  public static final String DEFAULT_NAME = "daffodil";
  public static final String OPERATOR_TYPE = "DAFFODIL_SUB_SCAN";

  public static class DaffodilReaderFactory extends FileReaderFactory {
    private final DaffodilBatchReader.DaffodilReaderConfig readerConfig;

    private final EasySubScan scan;

    public DaffodilReaderFactory(DaffodilBatchReader.DaffodilReaderConfig config, EasySubScan scan) {
      this.readerConfig = config;
      this.scan = scan;
    }

    @Override
    public ManagedReader newReader(FileSchemaNegotiator negotiator) {
      return new DaffodilBatchReader(readerConfig, scan, negotiator);
    }
  }

   public DaffodilFormatPlugin(String name, DrillbitContext context,
       Configuration fsConf, StoragePluginConfig storageConfig,
       DaffodilFormatConfig formatConfig) {
     super(name, easyConfig(fsConf, formatConfig), context, storageConfig, formatConfig);
   }

    private static EasyFormatConfig easyConfig(Configuration fsConf, DaffodilFormatConfig pluginConfig) {
    return EasyFormatConfig.builder()
        .readable(true)
        .writable(false)
        .blockSplittable(false)
        .compressible(true)
        .extensions(pluginConfig.getExtensions())
        .fsConf(fsConf)
        .readerOperatorType(OPERATOR_TYPE)
        .scanVersion(ScanFrameworkVersion.EVF_V2)
        .supportsLimitPushdown(true)
        .supportsProjectPushdown(true)
        .defaultName(DaffodilFormatPlugin.DEFAULT_NAME)
        .build();
  }

  @Override
  protected void configureScan(FileScanLifecycleBuilder builder, EasySubScan scan) {
    builder.nullType(Types.optional(MinorType.VARCHAR));
    builder.readerFactory(new DaffodilReaderFactory(formatConfig.getReaderConfig(this), scan));
  }
}
