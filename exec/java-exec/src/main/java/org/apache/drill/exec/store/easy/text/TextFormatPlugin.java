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
package org.apache.drill.exec.store.easy.text;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.MetadataProviderManager;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.planner.common.DrillStatsTable.TableStatistics;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsScanFramework.ColumnsScanBuilder;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileReaderFactory;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileScanBuilder;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasyGroupScan;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.drill.exec.store.dfs.easy.EasyWriter;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.store.easy.text.compliant.CompliantTextRecordReader;
import org.apache.drill.exec.store.easy.text.compliant.TextParsingSettings;
import org.apache.drill.exec.store.easy.text.compliant.v3.CompliantTextBatchReader;
import org.apache.drill.exec.store.easy.text.compliant.v3.TextParsingSettingsV3;
import org.apache.drill.exec.store.schedule.CompleteFileWork;
import org.apache.drill.exec.store.text.DrillTextRecordReader;
import org.apache.drill.exec.store.text.DrillTextRecordWriter;
import org.apache.drill.exec.vector.accessor.convert.AbstractConvertFromString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;

public class TextFormatPlugin extends EasyFormatPlugin<TextFormatPlugin.TextFormatConfig> {
  private final static String PLUGIN_NAME = "text";

  @JsonTypeName(PLUGIN_NAME)
  @JsonInclude(Include.NON_DEFAULT)
  public static class TextFormatConfig implements FormatPluginConfig {

    public List<String> extensions = ImmutableList.of();
    public String lineDelimiter = "\n";
    public char fieldDelimiter = '\n';
    public char quote = '"';
    public char escape = '"';
    public char comment = '#';
    public boolean skipFirstLine = false;
    public boolean extractHeader = false;

    public TextFormatConfig() { }

    public List<String> getExtensions() { return extensions; }
    public char getQuote() { return quote; }
    public char getEscape() { return escape; }
    public char getComment() { return comment; }
    public String getLineDelimiter() { return lineDelimiter; }
    public char getFieldDelimiter() { return fieldDelimiter; }
    public boolean isSkipFirstLine() { return skipFirstLine; }

    @JsonIgnore
    public boolean isHeaderExtractionEnabled() { return extractHeader; }

    @JsonIgnore
    public String getFieldDelimiterAsString(){
      return new String(new char[]{fieldDelimiter});
    }

    @Deprecated
    @JsonProperty("delimiter")
    public void setFieldDelimiter(char delimiter){
      this.fieldDelimiter = delimiter;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + comment;
      result = prime * result + escape;
      result = prime * result + ((extensions == null) ? 0 : extensions.hashCode());
      result = prime * result + fieldDelimiter;
      result = prime * result + ((lineDelimiter == null) ? 0 : lineDelimiter.hashCode());
      result = prime * result + quote;
      result = prime * result + (skipFirstLine ? 1231 : 1237);
      result = prime * result + (extractHeader ? 1231 : 1237);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      TextFormatConfig other = (TextFormatConfig) obj;
      if (comment != other.comment) {
        return false;
      }
      if (escape != other.escape) {
        return false;
      }
      if (extensions == null) {
        if (other.extensions != null) {
          return false;
        }
      } else if (!extensions.equals(other.extensions)) {
        return false;
      }
      if (fieldDelimiter != other.fieldDelimiter) {
        return false;
      }
      if (lineDelimiter == null) {
        if (other.lineDelimiter != null) {
          return false;
        }
      } else if (!lineDelimiter.equals(other.lineDelimiter)) {
        return false;
      }
      if (quote != other.quote) {
        return false;
      }
      if (skipFirstLine != other.skipFirstLine) {
        return false;
      }
      if (extractHeader != other.extractHeader) {
        return false;
      }
      return true;
    }
  }

  /**
   * Builds the readers for the V3 text scan operator.
   */
  private static class ColumnsReaderFactory extends FileReaderFactory {

    private final TextFormatPlugin plugin;

    public ColumnsReaderFactory(TextFormatPlugin plugin) {
      this.plugin = plugin;
    }

    @Override
    public ManagedReader<? extends FileSchemaNegotiator> newReader(
        FileSplit split) {
      TextParsingSettingsV3 settings = new TextParsingSettingsV3();
      settings.set(plugin.getConfig());
      return new CompliantTextBatchReader(split, fileSystem(), settings);
    }
  }

  /**
   * Builds the V3 text scan operator.
   */
  private static class TextScanBatchCreator extends ScanFrameworkCreator {

    private final TextFormatPlugin textPlugin;

    public TextScanBatchCreator(TextFormatPlugin plugin) {
      super(plugin);
      textPlugin = plugin;
    }

    @Override
    protected FileScanBuilder frameworkBuilder(
        EasySubScan scan) throws ExecutionSetupException {
      ColumnsScanBuilder builder = new ColumnsScanBuilder();
      builder.setReaderFactory(new ColumnsReaderFactory(textPlugin));

      // If this format has no headers, or wants to skip them,
      // then we must use the columns column to hold the data.

      builder.requireColumnsArray(
          ! textPlugin.getConfig().isHeaderExtractionEnabled());

      // Text files handle nulls in an unusual way. Missing columns
      // are set to required Varchar and filled with blanks. Yes, this
      // means that the SQL statement or code cannot differentiate missing
      // columns from empty columns, but that is how CSV and other text
      // files have been defined within Drill.

      builder.setNullType(
          MajorType.newBuilder()
            .setMinorType(MinorType.VARCHAR)
            .setMode(DataMode.REQUIRED)
            .build());

      // Pass along the output schema, if any

      builder.setOutputSchema(scan.getSchema());

      // CSV maps blank columns to nulls (for nullable non-string columns),
      // or to the default value (for non-nullable non-string columns.)

      builder.setConversionProperty(AbstractConvertFromString.BLANK_ACTION_PROP,
          AbstractConvertFromString.BLANK_AS_NULL);

      return builder;
    }
  }

  public TextFormatPlugin(String name, DrillbitContext context, Configuration fsConf, StoragePluginConfig storageConfig) {
     this(name, context, fsConf, storageConfig, new TextFormatConfig());
  }

  public TextFormatPlugin(String name, DrillbitContext context, Configuration fsConf, StoragePluginConfig config,
      TextFormatConfig formatPluginConfig) {
    super(name, easyConfig(fsConf, formatPluginConfig), context, config, formatPluginConfig);
  }

  private static EasyFormatConfig easyConfig(Configuration fsConf, TextFormatConfig pluginConfig) {
    EasyFormatConfig config = new EasyFormatConfig();
    config.readable = true;
    config.writable = true;
    config.blockSplittable = true;
    config.compressible = true;
    config.supportsProjectPushdown = true;
    config.extensions = pluginConfig.getExtensions();
    config.fsConf = fsConf;
    config.defaultName = PLUGIN_NAME;
    config.readerOperatorType = CoreOperatorType.TEXT_SUB_SCAN_VALUE;
    config.writerOperatorType = CoreOperatorType.TEXT_WRITER_VALUE;
    return config;
  }

  @Override
  public AbstractGroupScan getGroupScan(String userName, FileSelection selection, List<SchemaPath> columns, MetadataProviderManager metadataProviderManager)
      throws IOException {
    return new EasyGroupScan(userName, selection, this, columns, selection.selectionRoot, metadataProviderManager);
  }

  @Override
  public AbstractGroupScan getGroupScan(String userName, FileSelection selection,
      List<SchemaPath> columns, OptionManager options, MetadataProviderManager metadataProviderManager) throws IOException {
    return new EasyGroupScan(userName, selection, this, columns,
        selection.selectionRoot,
        // Some paths provide a null option manager. In that case, default to a
        // min width of 1; just like the base class.
        options == null ? 1 : (int) options.getLong(ExecConstants.MIN_READER_WIDTH_KEY), metadataProviderManager);
  }

  @Override
  protected ScanBatchCreator scanBatchCreator(OptionManager options) {
    // Create the "legacy", "V2" reader or the new "V3" version based on
    // the result set loader. This code should be temporary: the two
    // readers provide identical functionality for the user; only the
    // internals differ.
    if (options.getBoolean(ExecConstants.ENABLE_V3_TEXT_READER_KEY)) {
      return new TextScanBatchCreator(this);
    } else {
      return new ClassicScanBatchCreator(this);
    }
  }

  // TODO: Remove this once the V2 reader is removed.
  @Override
  public RecordReader getRecordReader(FragmentContext context,
                                      DrillFileSystem dfs,
                                      FileWork fileWork,
                                      List<SchemaPath> columns,
                                      String userName) {
    Path path = dfs.makeQualified(fileWork.getPath());
    FileSplit split = new FileSplit(path, fileWork.getStart(), fileWork.getLength(), new String[]{""});

    if (context.getOptions().getBoolean(ExecConstants.ENABLE_NEW_TEXT_READER_KEY)) {
      TextParsingSettings settings = new TextParsingSettings();
      settings.set(formatConfig);
      return new CompliantTextRecordReader(split, dfs, settings, columns);
    } else {
      char delim = formatConfig.getFieldDelimiter();
      return new DrillTextRecordReader(split, dfs.getConf(), context, delim, columns);
    }
  }

  @Override
  public RecordWriter getRecordWriter(final FragmentContext context, final EasyWriter writer) throws IOException {
    final Map<String, String> options = new HashMap<>();

    options.put("location", writer.getLocation());
    FragmentHandle handle = context.getHandle();
    String fragmentId = String.format("%d_%d", handle.getMajorFragmentId(), handle.getMinorFragmentId());
    options.put("prefix", fragmentId);
    options.put("separator", getConfig().getFieldDelimiterAsString());
    options.put("extension", getConfig().getExtensions().get(0));

    RecordWriter recordWriter = new DrillTextRecordWriter(
        context.getAllocator(), writer.getStorageStrategy(), writer.getFormatPlugin().getFsConf());
    recordWriter.init(options);

    return recordWriter;
  }

  @Override
  public boolean supportsStatistics() {
    return false;
  }

  @Override
  public TableStatistics readStatistics(FileSystem fs, Path statsTablePath) {
    throw new UnsupportedOperationException("unimplemented");
  }

  @Override
  public void writeStatistics(TableStatistics statistics, FileSystem fs, Path statsTablePath) {
    throw new UnsupportedOperationException("unimplemented");
  }

  @Override
  protected ScanStats getScanStats(final PlannerSettings settings, final EasyGroupScan scan) {
    long data = 0;
    for (final CompleteFileWork work : scan.getWorkIterable()) {
      data += work.getTotalBytes();
    }
    final double estimatedRowSize = settings.getOptions().getOption(ExecConstants.TEXT_ESTIMATED_ROW_SIZE);
    final double estRowCount = data / estimatedRowSize;
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, (long) estRowCount, 1, data);
  }
}
