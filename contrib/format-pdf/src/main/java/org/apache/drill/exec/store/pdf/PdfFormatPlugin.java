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

package org.apache.drill.exec.store.pdf;

import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileReaderFactory;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileScanLifecycleBuilder;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.hadoop.conf.Configuration;


public class PdfFormatPlugin extends EasyFormatPlugin<PdfFormatConfig> {

  protected static final String DEFAULT_NAME = "pdf";

  private static class PdfReaderFactory extends FileReaderFactory {
    private final PdfBatchReader.PdfReaderConfig readerConfig;

    public PdfReaderFactory(PdfBatchReader.PdfReaderConfig config) {
      readerConfig = config;
    }

    @Override
    public ManagedReader newReader(FileSchemaNegotiator negotiator) {
      return new PdfBatchReader(readerConfig, negotiator);
    }
  }

  public PdfFormatPlugin(String name, DrillbitContext context,
                           Configuration fsConf, StoragePluginConfig storageConfig,
                           PdfFormatConfig formatConfig) {
    super(name, easyConfig(fsConf, formatConfig), context, storageConfig, formatConfig);
  }

  private static EasyFormatPlugin.EasyFormatConfig easyConfig(Configuration fsConf, PdfFormatConfig pluginConfig) {
    return EasyFormatConfig.builder()
      .readable(true)
      .writable(false)
      .blockSplittable(false)
      .compressible(true)
      .supportsProjectPushdown(true)
      .extensions(pluginConfig.extensions())
      .fsConf(fsConf)
      .defaultName(DEFAULT_NAME)
      .scanVersion(ScanFrameworkVersion.EVF_V2)
      .supportsLimitPushdown(true)
      .build();
  }

  @Override
  protected void configureScan(FileScanLifecycleBuilder builder, EasySubScan scan) {
    PdfBatchReader.PdfReaderConfig readerConfig = new PdfBatchReader.PdfReaderConfig(this);
    builder.nullType(Types.optional(TypeProtos.MinorType.VARCHAR));
    builder.readerFactory(new PdfReaderFactory(readerConfig));
  }
}
