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

import org.apache.drill.common.exceptions.ChildErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.metastore.MetadataProviderManager;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader.EarlyEofException;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileReaderFactory;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileScanLifecycleBuilder;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileSchemaNegotiator;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
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
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
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
public class TextFormatPlugin extends EasyFormatPlugin<TextFormatConfig> {
  final static String PLUGIN_NAME = "text";

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
  public static final String TRIM_WHITESPACE_PROP = TEXT_PREFIX + "trim";
  public static final String PARSE_UNESCAPED_QUOTES_PROP = TEXT_PREFIX + "parseQuotes";

  public static final String WRITER_OPERATOR_TYPE = "TEXT_WRITER";

  private static class TextReaderFactory extends FileReaderFactory {
    private final TextParsingSettings settings;

    public TextReaderFactory(TextParsingSettings settings) {
      this.settings = settings;
    }

    @Override
    public ManagedReader newReader(FileSchemaNegotiator negotiator) throws EarlyEofException {
       return new CompliantTextBatchReader(negotiator, settings);
    }
  }

  public TextFormatPlugin(String name, DrillbitContext context,
      Configuration fsConf, StoragePluginConfig config,
      TextFormatConfig formatPluginConfig) {
    super(name, easyConfig(fsConf, formatPluginConfig), context, config, formatPluginConfig);
  }

  private static EasyFormatConfig easyConfig(Configuration fsConf, TextFormatConfig pluginConfig) {
    return EasyFormatConfig.builder()
        .readable(true)
        .writable(true)
        .blockSplittable(true)
        .compressible(true)
        .supportsProjectPushdown(true)
        .extensions(pluginConfig.getExtensions())
        .fsConf(fsConf)
        .defaultName(PLUGIN_NAME)
        .writerOperatorType(WRITER_OPERATOR_TYPE)
        .scanVersion(ScanFrameworkVersion.EVF_V2)
        .supportsLimitPushdown(true)
        .build();
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
  protected void configureScan(FileScanLifecycleBuilder builder, EasySubScan scan) {
    TextParsingSettings settings =
        new TextParsingSettings(getConfig(), scan.getSchema());
    builder.readerFactory(new TextReaderFactory(settings));

    // Text files handle nulls in an unusual way. Missing columns
    // are set to required Varchar and filled with blanks. Yes, this
    // means that the SQL statement or code cannot differentiate missing
    // columns from empty columns, but that is how CSV and other text
    // files have been defined within Drill.
    builder.nullType(Types.required(MinorType.VARCHAR));

    // Provide custom error context
    builder.errorContext(
        new ChildErrorContext(builder.errorContext()) {
          @Override
          public void addContext(UserException.Builder builder) {
            super.addContext(builder);
            builder.addContext("Extract headers",
                Boolean.toString(getConfig().isHeaderExtractionEnabled()));
            builder.addContext("Skip first line",
                Boolean.toString(getConfig().isSkipFirstLine()));
          }
        });
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
