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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.exceptions.ChildErrorContext;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.metastore.MetadataProviderManager;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsScanFramework.ColumnsScanBuilder;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileReaderFactory;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileScanBuilder;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.record.metadata.Propertied;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasyGroupScan;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.drill.exec.store.dfs.easy.EasyWriter;
import org.apache.drill.exec.store.easy.text.reader.CompliantTextBatchReader;
import org.apache.drill.exec.store.easy.text.reader.TextParsingSettings;
import org.apache.drill.exec.store.easy.text.writer.TextRecordWriter;
import org.apache.drill.exec.store.schedule.CompleteFileWork;
import org.apache.drill.exec.vector.accessor.convert.AbstractConvertFromString;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Text format plugin for CSV and other delimited text formats.
 * Allows use of a "provided schema", including using table properties
 * on that schema to override "static" ("or default") properties
 * defined in the plugin config. Allows, say, having ".csv" files
 * in which some have no headers (the default) and some do have
 * headers (as specified via table properties in the provided schema.)
 * <p>
 * Makes use of the scan framework and the result set loader mechanism
 * to allow tight control of the size of produced batches (as well
 * as to support provided schema.)
 */

public class TextFormatPlugin extends EasyFormatPlugin<TextFormatPlugin.TextFormatConfig> {
  private final static String PLUGIN_NAME = "text";

  public static final int MAXIMUM_NUMBER_COLUMNS = 64 * 1024;

  public static final int MAX_CHARS_PER_COLUMN = Character.MAX_VALUE;

  public static final char NULL_CHAR = '\0';

  // Provided schema table properties unique to this plugin. If specified
  // in the provided schema, they override the corresponding property in
  // the plugin config. Names here match the field names in the format config.
  // The "text." intermediate name avoids potential conflicts with other
  // uses of these names and denotes that the names work only for the text
  // format plugin.

  public static final String TEXT_PREFIX = Propertied.pluginPrefix(PLUGIN_NAME);
  public static final String HAS_HEADERS_PROP = TEXT_PREFIX + "extractHeader";
  public static final String SKIP_FIRST_LINE_PROP = TEXT_PREFIX + "skipFirstLine";
  public static final String DELIMITER_PROP = TEXT_PREFIX + "fieldDelimiter";
  public static final String COMMENT_CHAR_PROP = TEXT_PREFIX + "comment";
  public static final String QUOTE_PROP = TEXT_PREFIX + "quote";
  public static final String QUOTE_ESCAPE_PROP = TEXT_PREFIX + "escape";
  public static final String LINE_DELIM_PROP = TEXT_PREFIX + "lineDelimiter";

  @JsonTypeName(PLUGIN_NAME)
  @JsonInclude(Include.NON_DEFAULT)
  public static class TextFormatConfig implements FormatPluginConfig {

    public List<String> extensions = Collections.emptyList();
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

    private final TextParsingSettings settings;

    public ColumnsReaderFactory(TextParsingSettings settings) {
      this.settings = settings;
    }

    @Override
    public ManagedReader<? extends FileSchemaNegotiator> newReader() {
       return new CompliantTextBatchReader(settings);
    }
  }

  public TextFormatPlugin(String name, DrillbitContext context, Configuration fsConf, StoragePluginConfig storageConfig) {
     this(name, context, fsConf, storageConfig, new TextFormatConfig());
  }

  public TextFormatPlugin(String name, DrillbitContext context,
      Configuration fsConf, StoragePluginConfig config,
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
    config.useEnhancedScan = true;
    return config;
  }

  @Override
  public AbstractGroupScan getGroupScan(String userName, FileSelection selection,
      List<SchemaPath> columns, MetadataProviderManager metadataProviderManager)
      throws IOException {
    return new EasyGroupScan(userName, selection, this, columns, selection.selectionRoot, metadataProviderManager);
  }

  @Override
  public AbstractGroupScan getGroupScan(String userName, FileSelection selection,
      List<SchemaPath> columns, OptionManager options,
      MetadataProviderManager metadataProviderManager) throws IOException {
    return new EasyGroupScan(userName, selection, this, columns,
        selection.selectionRoot,
        // Some paths provide a null option manager. In that case, default to a
        // min width of 1; just like the base class.
        options == null ? 1 : (int) options.getLong(ExecConstants.MIN_READER_WIDTH_KEY), metadataProviderManager);
  }

  @Override
  protected FileScanBuilder frameworkBuilder(
      OptionManager options, EasySubScan scan) throws ExecutionSetupException {
    ColumnsScanBuilder builder = new ColumnsScanBuilder();
    initScanBuilder(builder, scan);
    TextParsingSettings settings =
        new TextParsingSettings(getConfig(), scan, options);
    builder.setReaderFactory(new ColumnsReaderFactory(settings));

    // If this format has no headers, or wants to skip them,
    // then we must use the columns column to hold the data.

    builder.requireColumnsArray(settings.isUseRepeatedVarChar());

    // Text files handle nulls in an unusual way. Missing columns
    // are set to required Varchar and filled with blanks. Yes, this
    // means that the SQL statement or code cannot differentiate missing
    // columns from empty columns, but that is how CSV and other text
    // files have been defined within Drill.

    builder.setNullType(Types.required(MinorType.VARCHAR));

    // CSV maps blank columns to nulls (for nullable non-string columns),
    // or to the default value (for non-nullable non-string columns.)

    builder.typeConverterBuilder().setConversionProperty(
        AbstractConvertFromString.BLANK_ACTION_PROP,
        AbstractConvertFromString.BLANK_AS_NULL);

    // The text readers use required Varchar columns to represent null columns.

    builder.allowRequiredNullColumns(true);

    // Provide custom error context

    builder.setContext(
        new ChildErrorContext(builder.errorContext()) {
          @Override
          public void addContext(UserException.Builder builder) {
            super.addContext(builder);
            builder.addContext("Extract headers:",
                Boolean.toString(getConfig().isHeaderExtractionEnabled()));
            builder.addContext("Skip first line:",
                Boolean.toString(getConfig().isSkipFirstLine()));
          }
        });

    return builder;
  }

  @Override
  public RecordWriter getRecordWriter(final FragmentContext context, final EasyWriter writer) throws IOException {
    Map<String, String> options = new HashMap<>();

    options.put("location", writer.getLocation());

    TextFormatConfig config = getConfig();
    List<String> extensions = config.getExtensions();
    options.put("extension", extensions == null || extensions.isEmpty() ? null : extensions.get(0));

    FragmentHandle handle = context.getHandle();
    String fragmentId = String.format("%d_%d", handle.getMajorFragmentId(), handle.getMinorFragmentId());
    options.put("prefix", fragmentId);

    options.put("addHeader", Boolean.toString(context.getOptions().getBoolean(ExecConstants.TEXT_WRITER_ADD_HEADER)));
    options.put("forceQuotes", Boolean.toString(context.getOptions().getBoolean(ExecConstants.TEXT_WRITER_FORCE_QUOTES)));

    options.put("lineSeparator", config.getLineDelimiter());
    options.put("fieldDelimiter", String.valueOf(config.getFieldDelimiter()));
    options.put("quote", String.valueOf(config.getQuote()));
    options.put("escape", String.valueOf(config.getEscape()));

    RecordWriter recordWriter = new TextRecordWriter(
        context.getAllocator(), writer.getStorageStrategy(), writer.getFormatPlugin().getFsConf());
    recordWriter.init(options);

    return recordWriter;
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
