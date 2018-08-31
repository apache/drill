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

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.easy.json.JsonProcessor.ReadState;
import org.apache.drill.exec.store.easy.json.reader.CountingJsonReader;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;

public class JSONRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JSONRecordReader.class);

  public static final long DEFAULT_ROWS_PER_BATCH = BaseValueVector.INITIAL_VALUE_ALLOCATION;

  private VectorContainerWriter writer;

  // Data we're consuming
  private Path hadoopPath;
  private JsonNode embeddedContent;
  private InputStream stream;
  private final DrillFileSystem fileSystem;
  private JsonProcessor jsonReader;
  private int recordCount;
  private long runningRecordCount = 0;
  private final FragmentContext fragmentContext;
  private final boolean enableAllTextMode;
  private final boolean enableNanInf;
  private final boolean readNumbersAsDouble;
  private final boolean unionEnabled;
  private long parseErrorCount;
  private final boolean skipMalformedJSONRecords;
  private final boolean printSkippedMalformedJSONRecordLineNumber;
  ReadState write = null;

  /**
   * Create a JSON Record Reader that uses a file based input stream.
   * @param fragmentContext
   * @param inputPath
   * @param fileSystem
   * @param columns  pathnames of columns/subfields to read
   * @throws OutOfMemoryException
   */
  public JSONRecordReader(final FragmentContext fragmentContext, final String inputPath, final DrillFileSystem fileSystem,
      final List<SchemaPath> columns) throws OutOfMemoryException {
    this(fragmentContext, inputPath, null, fileSystem, columns);
  }

  /**
   * Create a new JSON Record Reader that uses a in memory materialized JSON stream.
   * @param fragmentContext
   * @param embeddedContent
   * @param fileSystem
   * @param columns  pathnames of columns/subfields to read
   * @throws OutOfMemoryException
   */
  public JSONRecordReader(final FragmentContext fragmentContext, final JsonNode embeddedContent,
      final DrillFileSystem fileSystem, final List<SchemaPath> columns) throws OutOfMemoryException {
    this(fragmentContext, null, embeddedContent, fileSystem, columns);
  }

  private JSONRecordReader(final FragmentContext fragmentContext, final String inputPath,
      final JsonNode embeddedContent, final DrillFileSystem fileSystem,
      final List<SchemaPath> columns) {

    Preconditions.checkArgument(
        (inputPath == null && embeddedContent != null) ||
        (inputPath != null && embeddedContent == null),
        "One of inputPath or embeddedContent must be set but not both."
        );

    if (inputPath != null) {
      this.hadoopPath = new Path(inputPath);
    } else {
      this.embeddedContent = embeddedContent;
    }

    this.fileSystem = fileSystem;
    this.fragmentContext = fragmentContext;
    // only enable all text mode if we aren't using embedded content mode.
    this.enableAllTextMode = embeddedContent == null && fragmentContext.getOptions().getOption(ExecConstants.JSON_READER_ALL_TEXT_MODE_VALIDATOR);
    this.enableNanInf = fragmentContext.getOptions().getOption(ExecConstants.JSON_READER_NAN_INF_NUMBERS_VALIDATOR);
    this.readNumbersAsDouble = embeddedContent == null && fragmentContext.getOptions().getOption(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE_VALIDATOR);
    this.unionEnabled = embeddedContent == null && fragmentContext.getOptions().getBoolean(ExecConstants.ENABLE_UNION_TYPE_KEY);
    this.skipMalformedJSONRecords = fragmentContext.getOptions().getOption(ExecConstants.JSON_SKIP_MALFORMED_RECORDS_VALIDATOR);
    this.printSkippedMalformedJSONRecordLineNumber = fragmentContext.getOptions().getOption(ExecConstants.JSON_READER_PRINT_INVALID_RECORDS_LINE_NOS_FLAG_VALIDATOR);
    setColumns(columns);
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
  public void setup(final OperatorContext context, final OutputMutator output) throws ExecutionSetupException {
    try{
      if (hadoopPath != null) {
        this.stream = fileSystem.openPossiblyCompressedStream(hadoopPath);
      }

      this.writer = new VectorContainerWriter(output, unionEnabled);
      if (isSkipQuery()) {
        this.jsonReader = new CountingJsonReader(fragmentContext.getManagedBuffer(), enableNanInf);
      } else {
        this.jsonReader = new JsonReader.Builder(fragmentContext.getManagedBuffer())
            .schemaPathColumns(ImmutableList.copyOf(getColumns()))
            .allTextMode(enableAllTextMode)
            .skipOuterList(true)
            .readNumbersAsDouble(readNumbersAsDouble)
            .enableNanInf(enableNanInf)
            .build();
      }
      setupParser();
    } catch (final Exception e){
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
    } else {
      jsonReader.setSource(embeddedContent);
    }
    jsonReader.setIgnoreJSONParseErrors(skipMalformedJSONRecords);
  }

  protected void handleAndRaise(String suffix, Exception e) throws UserException {

    String message = e.getMessage();
    int columnNr = -1;

    if (e instanceof JsonParseException) {
      final JsonParseException ex = (JsonParseException) e;
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
            logger.debug("Error parsing JSON in " + hadoopPath.getName() + " : line nos :" + (recordCount + parseErrorCount));
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

  @Override
  public void close() throws Exception {
    if(stream != null) {
      stream.close();
    }
  }
}
