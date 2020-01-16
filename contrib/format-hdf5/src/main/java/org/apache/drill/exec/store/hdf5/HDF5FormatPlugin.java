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

package org.apache.drill.exec.store.hdf5;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileScanBuilder;

import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.drill.shaded.guava.com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.hdf5.HDF5BatchReader.HDF5ReaderConfig;

import java.io.File;


public class HDF5FormatPlugin extends EasyFormatPlugin<HDF5FormatConfig> {

  public static final String DEFAULT_NAME = "hdf5";

  private final DrillbitContext context;

  public HDF5FormatPlugin(String name, DrillbitContext context,
                          Configuration fsConf,
                          StoragePluginConfig storageConfig,
                          HDF5FormatConfig formatConfig) {
    super(name, easyConfig(fsConf, formatConfig), context, storageConfig, formatConfig);
    this.context = context;
  }

  private static EasyFormatConfig easyConfig(Configuration fsConf, HDF5FormatConfig pluginConfig) {
    EasyFormatConfig config = new EasyFormatConfig();
    config.readable = true;
    config.writable = false;
    config.blockSplittable = false;
    config.compressible = true;
    config.supportsProjectPushdown = true;
    config.extensions = pluginConfig.getExtensions();
    config.fsConf = fsConf;
    config.defaultName = DEFAULT_NAME;
    config.readerOperatorType = UserBitShared.CoreOperatorType.HDF5_SUB_SCAN_VALUE;
    config.useEnhancedScan = true;
    return config;
  }

  @Override
  protected FileScanBuilder frameworkBuilder(OptionManager options, EasySubScan scan) throws ExecutionSetupException {
    FileScanBuilder builder = new FileScanBuilder();

    builder.setReaderFactory(new HDF5ReaderFactory(new HDF5BatchReader.HDF5ReaderConfig(this, formatConfig)));
    initScanBuilder(builder, scan);
    builder.setNullType(Types.optional(TypeProtos.MinorType.VARCHAR));
    return builder;
  }

  public static class HDF5ReaderFactory extends FileScanFramework.FileReaderFactory {
    private final HDF5ReaderConfig readerConfig;

    HDF5ReaderFactory(HDF5ReaderConfig config) {
      readerConfig = config;
    }

    @Override
    public ManagedReader<? extends FileScanFramework.FileSchemaNegotiator> newReader() {
      return new HDF5BatchReader(readerConfig);
    }
  }

  /**
   * First tries to get drill temporary directory value from from config ${drill.tmp-dir},
   * then checks environmental variable $DRILL_TMP_DIR.
   * If value is still missing, generates directory using {@link Files#createTempDir()}.
   *
   * @return drill temporary directory path
   */
  protected File getTmpDir() {
    DrillConfig config = context.getConfig();
    String drillTempDir;
    if (config.hasPath(ExecConstants.DRILL_TMP_DIR)) {
      drillTempDir = config.getString(ExecConstants.DRILL_TMP_DIR);
    } else {
      drillTempDir = System.getenv("DRILL_TMP_DIR");
    }

    if (drillTempDir == null) {
      return Files.createTempDir();
    }

    return new File(drillTempDir);
  }
}
