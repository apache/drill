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

package org.apache.drill.exec.store.log;

import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsScanFramework;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsScanFramework.ColumnsScanBuilder;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileReaderFactory;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileScanBuilder;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.Propertied;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.drill.exec.store.log.LogBatchReader.LogReaderConfig;
import org.apache.drill.shaded.guava.com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogFormatPlugin extends EasyFormatPlugin<LogFormatConfig> {
  private static final Logger logger = LoggerFactory.getLogger(LogFormatPlugin.class);

  public static final String PLUGIN_NAME = "logRegex";
  public static final String PROP_PREFIX = Propertied.pluginPrefix(PLUGIN_NAME);
  public static final String REGEX_PROP = PROP_PREFIX + "regex";
  public static final String MAX_ERRORS_PROP = PROP_PREFIX + "maxErrors";

  private static class LogReaderFactory extends FileReaderFactory {
    private final LogReaderConfig readerConfig;

    public LogReaderFactory(LogReaderConfig config) {
      readerConfig = config;
    }

    @Override
    public ManagedReader<? extends FileSchemaNegotiator> newReader() {
       return new LogBatchReader(readerConfig);
    }
  }

  public LogFormatPlugin(String name, DrillbitContext context,
                         Configuration fsConf, StoragePluginConfig storageConfig,
                         LogFormatConfig formatConfig) {
    super(name, easyConfig(fsConf, formatConfig), context, storageConfig, formatConfig);
  }

  private static EasyFormatConfig easyConfig(Configuration fsConf, LogFormatConfig pluginConfig) {
    EasyFormatConfig config = new EasyFormatConfig();
    config.readable = true;
    config.writable = false;
    // Should be block splitable, but logic not yet implemented.
    config.blockSplittable = false;
    config.compressible = true;
    config.supportsProjectPushdown = true;
    config.extensions = Collections.singletonList(pluginConfig.getExtension());
    config.fsConf = fsConf;
    config.defaultName = PLUGIN_NAME;
    config.readerOperatorType = CoreOperatorType.REGEX_SUB_SCAN_VALUE;
    config.useEnhancedScan = true;
    return config;
  }

  /**
   * Build a file scan framework for this plugin.
   * <p>
   * This plugin was created before the concept of "provided schema" was
   * available. This plugin does, however, support a provided schema and
   * table properties. The code here handles the various cases.
   * <p>
   * For the regex and max errors:
   * <ul>
   * <li>Use the table property, if a schema is provided and the table
   * property is set.</li>
   * <li>Else, use the format config property.</li>
   * </ul>
   * <p>
   * For columns:
   * <ul>
   * <li>If no schema is provided (or the schema contains only table
   * properties with no columns), use the column names and types from
   * the plugin config.</li>
   * <li>If a schema is provided, and the plugin defines columns, use
   * the column types from the provided schema. Columns are matched by
   * name. Provided schema type override any types specified in the
   * plugin.</li>
   * <li>If a schema is provided, and the plugin config defines no
   * columns, use the column names and types from the provided schema.
   * The columns are assumed to appear in the same order as regex
   * fields.</li>
   * <li>If the regex has more groups than either schema has columns,
   * fill the extras with field_n of type VARCHAR.</li>
   * </ul>
   * <p>
   * Typical use cases:
   * <ul>
   * <li>Minimum config: only a regex in either plugin config or table
   * properties.</li>
   * <li>Plugin config defines regex, field names and types. (The
   * typical approach in Drill 1.16 and before.</li>
   * <li>Plugin config defines the regex and field names. The provided
   * schema defines types. (Separates physical and logical table
   * definitions.</li>
   * <li>Provided schema defines the regex and columns. May simplify
   * configuration as all table information is in one place. Allows
   * different regex patterns for different tables of the same file
   * suffix.</li>
   * </ul>
   */

  @Override
  protected FileScanBuilder frameworkBuilder(
      OptionManager options, EasySubScan scan) throws ExecutionSetupException {

    // Pattern and schema identical across readers; define
    // up front.

    TupleMetadata providedSchema = scan.getSchema();
    Pattern pattern = setupPattern(providedSchema);

    // Use a dummy matcher to get the group count. Group count not
    // available from the pattern itself, oddly.

    Matcher m = pattern.matcher("dummy");
    int groupCount = m.groupCount();
    if (groupCount == 0) {
      throw UserException
        .validationError()
        .message("Regex property has no groups, see Java Pattern class for details.")
        .addContext("Plugin", PLUGIN_NAME)
        .build(logger);
    }

    boolean hasColumns = (providedSchema != null && providedSchema.size() > 0);
    boolean hasSchema = hasColumns || formatConfig.hasSchema();
    SchemaBuilder schemaBuilder = new SchemaBuilder();
    FileScanBuilder builder;
    if (hasSchema) {
      TupleMetadata outputSchema;
      if (!hasColumns) {

        // No provided schema, build from plugin config
        // (or table function.)

        outputSchema = defineOutputSchemaFromConfig(groupCount);
      } else if (groupCount <= providedSchema.size()) {

        // Is a provided schema with enough columns. Just
        // use it.

        outputSchema = providedSchema;
      } else {

        // Have a provided schema, but more groups than
        // provided columns. Make up additional columns.

        outputSchema = defineOutputSchemaFromProvided(providedSchema, groupCount);
      }
      defineReaderSchema(schemaBuilder, outputSchema);

      // Use the file framework to enable support for implicit and partition
      // columns.

      builder = new FileScanBuilder();
      initScanBuilder(builder, scan);
      builder.typeConverterBuilder().providedSchema(outputSchema);
    } else {

      // No schema provided;  use the columns framework to use the columns[] array
      // Also supports implicit and partition metadata.

      schemaBuilder.addArray(ColumnsScanFramework.COLUMNS_COL, MinorType.VARCHAR);
      ColumnsScanBuilder colScanBuilder = new ColumnsScanBuilder();
      initScanBuilder(colScanBuilder, scan);
      colScanBuilder.requireColumnsArray(true);
      colScanBuilder.allowOtherCols(true);
      builder = colScanBuilder;
    }

    // Pass along the class that will create a batch reader on demand for
    // each input file.

    builder.setReaderFactory(new LogReaderFactory(
        new LogReaderConfig(this, pattern, buildSchema(schemaBuilder),
            !hasSchema, groupCount, maxErrors(providedSchema))));

    // The default type of regex columns is nullable VarChar,
    // so let's use that as the missing column type.

    builder.setNullType(Types.optional(MinorType.VARCHAR));
    return builder;
  }

  /**
   * Define the output schema: the schema after type conversions.
   * Does not include the special columns as those are added only when
   * requested, and are always VARCHAR.
   *
   * @param capturingGroups the number of capturing groups in the regex
   * (the number that return fields)
   * @return a schema for the plugin config with names and types filled
   * in. This schema drives type conversions if no schema is provided
   * for the table
   */

  private TupleMetadata defineOutputSchemaFromConfig(int capturingGroups) {
    List<String> fields = formatConfig.getFieldNames();
    for (int i = fields.size(); i < capturingGroups; i++) {
      fields.add("field_" + i);
    }
    SchemaBuilder builder = new SchemaBuilder();
    for (int i = 0; i < capturingGroups; i++) {
      makeColumn(builder, fields.get(i), i);
    }
    TupleMetadata schema = builder.buildSchema();

    // Populate the date formats, if provided.

    if (formatConfig.getSchema() == null) {
      return schema;
    }
    for (int i = 0; i < formatConfig.getSchema().size(); i++) {
      ColumnMetadata col = schema.metadata(i);
      switch (col.type()) {
      case DATE:
      case TIMESTAMP:
      case TIME:
        break;
      default:
        continue;
      }
      String format = formatConfig.getDateFormat(i);
      if (format == null) {
        continue;
      }
      col.setProperty(ColumnMetadata.FORMAT_PROP, format);
    }
    return schema;
  }

  /**
   * Build the output schema from the provided schema. Occurs in the case
   * that the regex has more capturing groups than the schema has columns.
   * Add all the provided columns, then add default fields for the
   * remaining capturing groups.
   *
   * @param providedSchema the schema provided for the table
   * @param capturingGroups the number of groups in the regex
   * @return an output schema with the provided columns, plus default
   * columns for any missing columns
   */

  private TupleMetadata defineOutputSchemaFromProvided(
      TupleMetadata providedSchema, int capturingGroups) {
    assert capturingGroups >= providedSchema.size();
    SchemaBuilder builder = new SchemaBuilder();
    for (int i = 0; i < providedSchema.size(); i++) {
      builder.addColumn(providedSchema.metadata(i).copy());
    }
    for (int i = providedSchema.size(); i < capturingGroups; i++) {
      builder.addNullable("field_" + i, MinorType.VARCHAR);
    }
    return builder.buildSchema();
  }

  /**
   * Define the simplified reader schema: this is the format that the reader
   * understands. All columns are VARCHAR, and the reader can offer the
   * two special columns.
   *
   * @param outputSchema the output schema, defined above, with types
   * filled in from the plugin config
   * @return the reader schema: same column names, all columns of type
   * VARCHAR (which is what a regex produces), with special columns added.
   * The projection mechanism will pick all, some or none of these columns
   * to project, then will take the desired output type from the output
   * schema, providing any conversions needed
   */

  private void defineReaderSchema(SchemaBuilder builder, TupleMetadata outputSchema) {
    for (int i = 0; i < outputSchema.size(); i++) {
      builder.addNullable(outputSchema.metadata(i).name(), MinorType.VARCHAR);
    }
  }

  private TupleMetadata buildSchema(SchemaBuilder builder) {
    builder.addNullable(LogBatchReader.RAW_LINE_COL_NAME, MinorType.VARCHAR);
    builder.addNullable(LogBatchReader.UNMATCHED_LINE_COL_NAME, MinorType.VARCHAR);
    TupleMetadata schema = builder.buildSchema();

    // Exclude special columns from wildcard expansion

    schema.metadata(LogBatchReader.RAW_LINE_COL_NAME).setBooleanProperty(
        ColumnMetadata.EXCLUDE_FROM_WILDCARD, true);
    schema.metadata(LogBatchReader.UNMATCHED_LINE_COL_NAME).setBooleanProperty(
        ColumnMetadata.EXCLUDE_FROM_WILDCARD, true);

    return schema;
  }

  /**
   * Determine the regex to use. Use the one from the REGEX_PROP table
   * property of the provided schema, if available. Else, use the one from
   * the plugin config. Then compile the regex. Issue an error if the
   * pattern is bad.
   */

  private Pattern setupPattern(TupleMetadata providedSchema) {
    String regex = formatConfig.getRegex();
    if (providedSchema != null) {
      regex = providedSchema.property(REGEX_PROP, regex);
    }
    if (Strings.isNullOrEmpty(regex)) {
      throw UserException
        .validationError()
        .message("Regex property is required")
        .addContext("Plugin", PLUGIN_NAME)
        .build(logger);
    }
    try {
      return Pattern.compile(regex);
    } catch (PatternSyntaxException e) {
      throw UserException
          .validationError(e)
          .message("Failed to parse regex: \"%s\"", regex)
          .build(logger);
    }
  }

  private void makeColumn(SchemaBuilder builder, String name, int patternIndex) {
    String typeName = formatConfig.getDataType(patternIndex);
    MinorType type;
    if (Strings.isNullOrEmpty(typeName)) {
      // No type name. VARCHAR is a safe guess
      type = MinorType.VARCHAR;
    } else {
      type = MinorType.valueOf(typeName.toUpperCase());
    }

    // Verify supported types
    switch (type) {
      case VARCHAR:
      case INT:
      case SMALLINT:
      case BIGINT:
      case FLOAT4:
      case FLOAT8:
      case DATE:
      case TIMESTAMP:
      case TIME:
        break;
      default:
        throw UserException
            .validationError()
            .message("Undefined column types")
            .addContext("Position", patternIndex)
            .addContext("Field name", name)
            .addContext("Type", typeName)
            .build(logger);
    }
    builder.addNullable(name, type);
  }

  public int maxErrors(TupleMetadata providedSchema) {
    if (providedSchema == null) {
      return formatConfig.getMaxErrors();
    }
    return providedSchema.intProperty(MAX_ERRORS_PROP,
        formatConfig.getMaxErrors());
  }
}
