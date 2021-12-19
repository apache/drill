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

package org.apache.drill.exec.store.httpd;

import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileReaderFactory;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader.EarlyEofException;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileScanLifecycleBuilder;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.hadoop.conf.Configuration;

public class HttpdLogFormatPlugin extends EasyFormatPlugin<HttpdLogFormatConfig> {

  protected static final String DEFAULT_NAME = "httpd";

  public static final String OPERATOR_TYPE = "HTPPD_LOG_SUB_SCAN";

  private static class HttpLogReaderFactory extends FileReaderFactory {

    private final HttpdLogFormatConfig config;
    private final EasySubScan scan;

    private HttpLogReaderFactory(HttpdLogFormatConfig config, EasySubScan scan) {
      this.config = config;
      this.scan = scan;
    }

    @Override
    public ManagedReader newReader(FileSchemaNegotiator negotiator) throws EarlyEofException {
      return new HttpdLogBatchReader(config, scan, negotiator);
    }
  }

  public HttpdLogFormatPlugin(final String name,
                              final DrillbitContext context,
                              final Configuration fsConf,
                              final StoragePluginConfig storageConfig,
                              final HttpdLogFormatConfig formatConfig) {

    super(name, easyConfig(fsConf, formatConfig), context, storageConfig, formatConfig);
  }

  private static EasyFormatConfig easyConfig(Configuration fsConf, HttpdLogFormatConfig pluginConfig) {
    return EasyFormatConfig.builder()
        .readable(true)
        .writable(false)
        .blockSplittable(false)
        .compressible(true)
        .supportsProjectPushdown(true)
        .extensions(pluginConfig.getExtensions())
        .fsConf(fsConf)
        .defaultName(DEFAULT_NAME)
        .readerOperatorType(OPERATOR_TYPE)
        .scanVersion(ScanFrameworkVersion.EVF_V2)
        .supportsLimitPushdown(true)
        .build();
  }

  @Override
  protected void configureScan(FileScanLifecycleBuilder builder, EasySubScan scan) {
    builder.nullType(Types.optional(TypeProtos.MinorType.VARCHAR));
    builder.readerFactory(new HttpLogReaderFactory(formatConfig, scan));
  }
}

