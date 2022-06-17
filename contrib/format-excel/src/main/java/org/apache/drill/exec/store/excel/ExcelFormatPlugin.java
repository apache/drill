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

package org.apache.drill.exec.store.excel;

import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileReaderFactory;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileScanLifecycleBuilder;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileSchemaNegotiator;

import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader.EarlyEofException;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.hadoop.conf.Configuration;

public class ExcelFormatPlugin extends EasyFormatPlugin<ExcelFormatConfig> {

  public static final String DEFAULT_NAME = "excel";
  public static final String OPERATOR_TYPE = "EXCEL_SUB_SCAN";

  private static class ExcelReaderFactory extends FileReaderFactory {
    private final ExcelBatchReader.ExcelReaderConfig readerConfig;
    private final EasySubScan scan;

    public ExcelReaderFactory(ExcelBatchReader.ExcelReaderConfig config, EasySubScan scan) {
      this.readerConfig = config;
      this.scan = scan;
    }

    @Override
    public ManagedReader newReader(FileSchemaNegotiator negotiator) throws EarlyEofException {
      return new ExcelBatchReader(readerConfig, scan, negotiator);
    }
  }

  public ExcelFormatPlugin(String name, DrillbitContext context,
                         Configuration fsConf, StoragePluginConfig storageConfig,
                         ExcelFormatConfig formatConfig) {
    super(name, easyConfig(fsConf, formatConfig), context, storageConfig, formatConfig);
  }

  private static EasyFormatConfig easyConfig(Configuration fsConf, ExcelFormatConfig pluginConfig) {
    return EasyFormatConfig.builder()
        .readable(true)
        .writable(false)
        .blockSplittable(false)
        .compressible(true)
        .extensions(pluginConfig.getExtensions())
        .fsConf(fsConf)
        .readerOperatorType(OPERATOR_TYPE)
        .defaultName(ExcelFormatPlugin.DEFAULT_NAME)
        .scanVersion(ScanFrameworkVersion.EVF_V2)
        .supportsProjectPushdown(true)
        .supportsLimitPushdown(true)
        .build();
  }

  @Override
  protected void configureScan(FileScanLifecycleBuilder builder, EasySubScan scan) {
    builder.nullType(Types.optional(TypeProtos.MinorType.VARCHAR));
    builder.readerFactory(new ExcelReaderFactory(formatConfig.getReaderConfig(this), scan));
  }
}
