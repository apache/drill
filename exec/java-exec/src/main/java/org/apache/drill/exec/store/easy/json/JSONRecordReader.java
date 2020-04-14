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
package org.apache.drill.exec.store.easy.json;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValue.OptionScope;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.easy.json.JSONFormatPlugin.JSONFormatConfig;
import org.apache.drill.exec.store.easy.json.JsonProcessor.ReadState;
import org.apache.drill.exec.store.easy.json.reader.CountingJsonReader;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Old-style JSON record reader. Not used when reading JSON files,
 * but is used by some "mini-plan" unit tests, and by the VALUES
 * reader. As a result, this reader cannot be removed and must be
 * maintained until the other uses are converted to the new-style
 * JSON reader.
 */
public class JSONRecordReader extends AbstractRecordReader {
  private static final Logger logger = LoggerFactory.getLogger(JSONRecordReader.class);

  public static final long DEFAULT_ROWS_PER_BATCH = BaseValueVector.INITIAL_VALUE_ALLOCATION;
  private static final OptionScope MIN_SCOPE = OptionScope.SESSION;

  private VectorContainerWriter writer;

  // Data we're consuming
  private Path hadoopPath;
  private JsonNode embeddedContent;
  private InputStream stream;
  private final DrillFileSystem fileSystem;
  private JsonProcessor jsonReader;
  private int recordCount;
  private long runningRecordCount;
  private final FragmentContext fragmentContext;
  private final boolean enableAllTextMode;
  private final boolean enableNanInf;
  private final boolean enableEscapeAnyChar;
  private final boolean readNumbersAsDouble;
  private final boolean unionEnabled;
  private long parseErrorCount;
  private final boolean skipMalformedJSONRecords;
  private final boolean printSkippedMalformedJSONRecordLineNumber;
  private final JSONFormatConfig config;
  private ReadState write;
  private InputStream inputStream;

  /**
   * Create a JSON Record Reader that uses a file based input stream.
   * @param fragmentContext the Drill fragment
   * @param inputPath the input path
   * @param fileSystem a Drill file system wrapper around the file system implementation
   * @param columns path names of columns/subfields to read
   * @param config The JSONFormatConfig for the storage plugin
   * @throws OutOfMemoryException If there is insufficient memory, Drill will throw an Out of Memory Exception
   */
  public JSONRecordReader(FragmentContext fragmentContext, Path inputPath, DrillFileSystem fileSystem,
      List<SchemaPath> columns, JSONFormatConfig config) throws OutOfMemoryException {
    this(fragmentContext, inputPath, null, fileSystem, columns, false, config);
  }

  /**
   * Create a new JSON Record Reader that uses an in memory materialized JSON stream.
   * @param fragmentContext the Drill fragment
   * @param embeddedContent embedded content
   * @param fileSystem a Drill file system wrapper around the file system implementation
   * @param columns path names of columns/subfields to read
   * @throws OutOfMemoryException If Drill runs out of memory, OME will be thrown
   */
  public JSONRecordReader(FragmentContext fragmentContext, JsonNode embeddedContent, DrillFileSystem fileSystem,
      List<SchemaPath> columns) throws OutOfMemoryException {
    this(fragmentContext, null, embeddedContent, fileSystem, columns, false,
      new JSONFormatConfig(null,
        embeddedContent == null && fragmentContext.getOptions().getOption(ExecConstants.JSON_READER_ALL_TEXT_MODE_VALIDATOR),
        embeddedContent == null && fragmentContext.getOptions().getOption(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE_VALIDATOR),
        fragmentContext.getOptions().getOption(ExecConstants.JSON_SKIP_MALFORMED_RECORDS_VALIDATOR),
        fragmentContext.getOptions().getOption(ExecConstants.JSON_READER_ESCAPE_ANY_CHAR_VALIDATOR),
        fragmentContext.getOptions().getOption(ExecConstants.JSON_READER_NAN_INF_NUMBERS_VALIDATOR)));
  }

  /**
   * Create a JSON Record Reader that uses an InputStream directly
   * @param fragmentContext the Drill fragment
   * @param columns path names of columns/subfields to read
   * @throws OutOfMemoryException If there is insufficient memory, Drill will throw an Out of Memory Exception
   */
  public JSONRecordReader(FragmentContext fragmentContext, List<SchemaPath> columns) throws OutOfMemoryException {
    this(fragmentContext, null, null, null, columns, true,
      new JSONFormatConfig(null,
        fragmentContext.getOptions().getOption(ExecConstants.JSON_READER_ALL_TEXT_MODE_VALIDATOR),
        fragmentContext.getOptions().getOption(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE_VALIDATOR),
        fragmentContext.getOptions().getOption(ExecConstants.JSON_SKIP_MALFORMED_RECORDS_VALIDATOR),
        fragmentContext.getOptions().getOption(ExecConstants.JSON_READER_ESCAPE_ANY_CHAR_VALIDATOR),
        fragmentContext.getOptions().getOption(ExecConstants.JSON_READER_NAN_INF_NUMBERS_VALIDATOR)));
  }

  private JSONRecordReader(FragmentContext fragmentContext, Path inputPath, JsonNode embeddedContent,
      DrillFileSystem fileSystem, List<SchemaPath> columns, boolean hasInputStream, JSONFormatConfig config) {

    Preconditions.checkArgument(
        (inputPath == null && embeddedContent != null && !hasInputStream) ||
        (inputPath != null && embeddedContent == null && !hasInputStream) ||
          (inputPath == null && embeddedContent == null && hasInputStream),
      "One of inputPath, inputStream or embeddedContent must be set but not all."
        );

    OptionManager contextOpts = fragmentContext.getOptions();

    if (inputPath != null) {
      this.hadoopPath = inputPath;
    } else {
      this.embeddedContent = embeddedContent;
    }

    // If the config is null, create a temporary one with the global options.
    if (config == null) {
      this.config = new JSONFormatConfig(null,
        embeddedContent == null && fragmentContext.getOptions().getOption(ExecConstants.JSON_READER_ALL_TEXT_MODE_VALIDATOR),
        embeddedContent == null && fragmentContext.getOptions().getOption(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE_VALIDATOR),
        fragmentContext.getOptions().getOption(ExecConstants.JSON_SKIP_MALFORMED_RECORDS_VALIDATOR),
        fragmentContext.getOptions().getOption(ExecConstants.JSON_READER_ESCAPE_ANY_CHAR_VALIDATOR),
        fragmentContext.getOptions().getOption(ExecConstants.JSON_READER_NAN_INF_NUMBERS_VALIDATOR));
    } else {
      this.config = config;
    }

    this.fileSystem = fileSystem;
    this.fragmentContext = fragmentContext;

    this.enableAllTextMode = allTextMode(contextOpts);
    this.enableNanInf = nanInf(contextOpts);
    this.enableEscapeAnyChar = escapeAnyChar(contextOpts);
    this.readNumbersAsDouble = readNumbersAsDouble(contextOpts);
    this.unionEnabled = embeddedContent == null && fragmentContext.getOptions().getBoolean(ExecConstants.ENABLE_UNION_TYPE_KEY);
    this.skipMalformedJSONRecords = skipMalformedJSONRecords(contextOpts);
    this.printSkippedMalformedJSONRecordLineNumber = fragmentContext.getOptions().getOption(ExecConstants.JSON_READER_PRINT_INVALID_RECORDS_LINE_NOS_FLAG_VALIDATOR);
    setColumns(columns);
  }

  /**
   * Returns the value of the all text mode.  Values set in the format config will override global values.
   * @return The value of allTextMode
   */
  private boolean allTextMode(OptionManager contextOpts) {
    // only enable all text mode if we aren't using embedded content mode.
    boolean allTextMode = (Boolean) ObjectUtils.firstNonNull(
      contextOpts.getOption(ExecConstants.JSON_ALL_TEXT_MODE).getValueMinScope(MIN_SCOPE),
      config.getAllTextMode(),
      contextOpts.getBoolean(ExecConstants.JSON_ALL_TEXT_MODE)
    );

    return embeddedContent == null && allTextMode;
  }

  private boolean readNumbersAsDouble(OptionManager contextOpts) {
    boolean numbersAsDouble = (Boolean) ObjectUtils.firstNonNull(
      contextOpts.getOption(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE).getValueMinScope(MIN_SCOPE),
      config.getReadNumbersAsDouble(),
      contextOpts.getBoolean(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE)
    );

    return embeddedContent == null && numbersAsDouble;
  }

  private boolean skipMalformedJSONRecords(OptionManager contextOpts) {
    boolean skipMalformedRecords = (Boolean) ObjectUtils.firstNonNull(
      contextOpts.getOption(ExecConstants.JSON_READER_SKIP_INVALID_RECORDS_FLAG).getValueMinScope(MIN_SCOPE),
      config.getSkipMalformedJSONRecords(),
      contextOpts.getBoolean(ExecConstants.JSON_READER_SKIP_INVALID_RECORDS_FLAG)
    );
    return embeddedContent == null && skipMalformedRecords;
  }

  private boolean escapeAnyChar(OptionManager contextOpts) {
    boolean allowNaN = (Boolean) ObjectUtils.firstNonNull(
      contextOpts.getOption(ExecConstants.JSON_READER_ESCAPE_ANY_CHAR).getValueMinScope(MIN_SCOPE),
      config.getEscapeAnyChar(),
      contextOpts.getBoolean(ExecConstants.JSON_READER_ESCAPE_ANY_CHAR)
    );
    return embeddedContent == null && allowNaN;
  }

  private boolean nanInf(OptionManager contextOpts) {
    boolean allowNaN = (Boolean) ObjectUtils.firstNonNull(
      contextOpts.getOption(ExecConstants.JSON_READER_NAN_INF_NUMBERS).getValueMinScope(MIN_SCOPE),
      config.getNanInf(),
      contextOpts.getBoolean(ExecConstants.JSON_READER_NAN_INF_NUMBERS)
    );
    return embeddedContent == null && allowNaN;
  }

  @Override
  public String toString() {
    return super.toString()
        + "[hadoopPath = " + hadoopPath
        + ", currentRecord=" + currentRecordNumberInFile()
        + ", jsonReader=" + jsonReader
        + ", recordCount = " + recordCount
        + ", parseErrorCount = " + parseErrorCount
        + ", runningRecordCount = " + runningRecordCount + ", ...]";
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    try{
      if (hadoopPath != null) {
        stream = fileSystem.openPossiblyCompressedStream(hadoopPath);
      }

      writer = new VectorContainerWriter(output, unionEnabled);
      if (isSkipQuery()) {
        jsonReader = new CountingJsonReader(fragmentContext.getManagedBuffer(), enableNanInf, enableEscapeAnyChar);
      } else {
        this.jsonReader = new JsonReader.Builder(fragmentContext.getManagedBuffer())
            .schemaPathColumns(ImmutableList.copyOf(getColumns()))
            .allTextMode(enableAllTextMode)
            .skipOuterList(true)
            .readNumbersAsDouble(readNumbersAsDouble)
            .enableNanInf(enableNanInf)
            .enableEscapeAnyChar(enableEscapeAnyChar)
            .build();
      }
      setupParser();
    } catch (Exception e){
      handleAndRaise("Failure reading JSON file", e);
    }
  }

  @Override
  protected List<SchemaPath> getDefaultColumnsToRead() {
    return ImmutableList.of();
  }

  private void setupParser() throws IOException {
    if (hadoopPath != null) {
      jsonReader.setSource(stream);
    } else if (inputStream!= null) {
      jsonReader.setSource(inputStream);
    } else {
      jsonReader.setSource(embeddedContent);
    }
    jsonReader.setIgnoreJSONParseErrors(skipMalformedJSONRecords);
  }

  protected void handleAndRaise(String suffix, Exception e) throws UserException {

    String message = e.getMessage();
    int columnNr = -1;

    if (e instanceof JsonParseException) {
      JsonParseException ex = (JsonParseException) e;
      message = ex.getOriginalMessage();
      columnNr = ex.getLocation().getColumnNr();
    }

    UserException.Builder exceptionBuilder = UserException.dataReadError(e)
        .message("%s - %s", suffix, message);
    if (columnNr > 0) {
      exceptionBuilder.pushContext("Column ", columnNr);
    }

    if (hadoopPath != null) {
      exceptionBuilder.pushContext("Record ", currentRecordNumberInFile())
          .pushContext("File ", hadoopPath.toUri().getPath());
    }

    throw exceptionBuilder.build(logger);
  }

  private long currentRecordNumberInFile() {
    return runningRecordCount + recordCount + 1;
  }

  @Override
  public int next() {
    writer.allocate();
    writer.reset();
    recordCount = 0;
    parseErrorCount = 0;
    if (write == ReadState.JSON_RECORD_PARSE_EOF_ERROR) {
      return recordCount;
    }
    while (recordCount < DEFAULT_ROWS_PER_BATCH) {
      try {
        writer.setPosition(recordCount);
        write = jsonReader.write(writer);
        if (write == ReadState.WRITE_SUCCEED) {
          recordCount++;
        } else if (write == ReadState.JSON_RECORD_PARSE_ERROR || write == ReadState.JSON_RECORD_PARSE_EOF_ERROR) {
          if (!skipMalformedJSONRecords) {
            handleAndRaise("Error parsing JSON", new Exception());
          }
          ++parseErrorCount;
          if (printSkippedMalformedJSONRecordLineNumber) {
            logger.debug("Error parsing JSON in {}: line: {}",
                hadoopPath.getName(), recordCount + parseErrorCount);
          }
          if (write == ReadState.JSON_RECORD_PARSE_EOF_ERROR) {
            break;
          }
        } else {
          break;
        }
      } catch (IOException ex) {
        handleAndRaise("Error parsing JSON", ex);
      }
    }
    // Skip empty json file with 0 row.
    // Only when data source has > 0 row, ensure the batch has one field.
    if (recordCount > 0) {
      jsonReader.ensureAtLeastOneField(writer);
    }
    writer.setValueCount(recordCount);
    updateRunningCount();
    return recordCount;
  }

  private void updateRunningCount() {
    runningRecordCount += recordCount;
  }

  public void setInputStream(InputStream in) {
    this.inputStream = in;
  }

  @Override
  public void close() throws Exception {
    if (stream != null) {
      stream.close();
      stream = null;
    }

    if (inputStream != null) {
      inputStream.close();
      inputStream = null;
    }
  }
}
