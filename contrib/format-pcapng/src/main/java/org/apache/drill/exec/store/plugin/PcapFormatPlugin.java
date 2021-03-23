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
package org.apache.drill.exec.store.plugin;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileReaderFactory;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileScanBuilder;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedScanFramework;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.drill.exec.store.pcap.PcapBatchReader;
import org.apache.drill.exec.store.pcap.decoder.PacketDecoder;
import org.apache.drill.exec.store.pcapng.PcapngBatchReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Pattern;

public class PcapFormatPlugin extends EasyFormatPlugin<PcapFormatConfig> {

  static final Logger logger = LoggerFactory.getLogger(ManagedScanFramework.class);
  private static PacketDecoder.FileFormat fileFormat = PacketDecoder.FileFormat.UNKNOWN;

  public PcapFormatPlugin(String name,
                          DrillbitContext context,
                          Configuration fsConf,
                          StoragePluginConfig storageConfig,
                          PcapFormatConfig formatConfig) {
    super(name, easyConfig(fsConf, formatConfig), context, storageConfig, formatConfig);
  }

  private static EasyFormatConfig easyConfig(Configuration fsConf, PcapFormatConfig pluginConfig) {
    return EasyFormatConfig.builder()
        .readable(true)
        .writable(false)
        .blockSplittable(false)
        .compressible(true)
        .extensions(pluginConfig.getExtensions())
        .fsConf(fsConf)
        .useEnhancedScan(true)
        .supportsLimitPushdown(true)
        .supportsProjectPushdown(true)
        .defaultName(PcapFormatConfig.NAME)
        .build();
  }

  private static class PcapReaderFactory extends FileReaderFactory {

    private final PcapFormatConfig config;
    private final EasySubScan scan;

    public PcapReaderFactory(PcapFormatConfig config, EasySubScan scan) {
      this.config = config;
      this.scan = scan;
    }

    @Override
    public ManagedReader<? extends FileSchemaNegotiator> newReader() {
      fileFormat = fromMagicNumber(fileFramework);
      return createReader(scan, config);
    }
  }

  @Override
  public ManagedReader<? extends FileSchemaNegotiator> newBatchReader(EasySubScan scan, OptionManager options) {
    return createReader(scan, formatConfig);
  }

  private static ManagedReader<? extends FileSchemaNegotiator> createReader(EasySubScan scan, PcapFormatConfig config) {
    switch(fileFormat) {
      case PCAPNG: return new PcapngBatchReader(config, scan);
      case PCAP:
      case UNKNOWN:
      default: return new PcapBatchReader(config, scan.getMaxRecords());
    }
  }

  @Override
  protected FileScanBuilder frameworkBuilder(OptionManager options, EasySubScan scan) {
    FileScanBuilder builder = new FileScanBuilder();
    builder.setReaderFactory(new PcapReaderFactory(formatConfig, scan));

    initScanBuilder(builder, scan);
    builder.nullType(Types.optional(MinorType.VARCHAR));
    return builder;
  }

  /**
   * Helper method to detect PCAP or PCAPNG file format based on file Magic Number
   *
   * @param fileFramework for obtaining InputStream
   * @return PCAP/PCAPNG file format
   */
  private static PacketDecoder.FileFormat fromMagicNumber(FileScanFramework fileFramework) {
    FileScanFramework.FileSchemaNegotiatorImpl negotiator = (FileScanFramework.FileSchemaNegotiatorImpl) fileFramework.newNegotiator();
    DrillFileSystem dfs = negotiator.fileSystem();
    Path path = dfs.makeQualified(negotiator.split().getPath());
    try (InputStream inputStream = dfs.openPossiblyCompressedStream(path)) {
      PacketDecoder decoder = new PacketDecoder(inputStream);
      return decoder.getFileFormat();
    } catch (IOException io) {
      throw UserException
              .dataReadError(io)
              .addContext("File name:", path.toString())
              .build(logger);
    }
  }

  /**
   * Detects PCAP or PCAPNG file format based on file extension. Not used now due to {@link #fromMagicNumber} usage for
   * this purpose
   *
   * @param filePath to be scanned
   * @return PCAP/PCAPNG file format
   */
  private static PacketDecoder.FileFormat fromFileExtension(Path filePath) {
    Pattern pcap = Pattern.compile(".*\\.pcap");
    Pattern archivedPcap = Pattern.compile(".*\\.pcap\\..*");
    Pattern pcapng = Pattern.compile(".*\\.pcapng");
    Pattern archivedPcapNG = Pattern.compile(".*\\.pcapng\\..*");
    String fileName = filePath.toString();
    if (pcap.matcher(fileName).matches() || archivedPcap.matcher(fileName).matches()) {
      return PacketDecoder.FileFormat.PCAP;
    } else if (pcapng.matcher(fileName).matches() || archivedPcapNG.matcher(fileName).matches()) {
      return PacketDecoder.FileFormat.PCAPNG;
    }
    return PacketDecoder.FileFormat.UNKNOWN;
  }
}
