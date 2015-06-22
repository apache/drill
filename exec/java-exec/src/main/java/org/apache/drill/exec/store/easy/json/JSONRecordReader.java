/**
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
import org.apache.drill.exec.memory.OutOfMemoryException;
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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public class JSONRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JSONRecordReader.class);

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
  private OperatorContext operatorContext;
  private final boolean enableAllTextMode;
  private final boolean readNumbersAsDouble;

  /**
   * Create a JSON Record Reader that uses a file based input stream.
   * @param fragmentContext
   * @param inputPath
   * @param fileSystem
   * @param columns
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
   * @param columns
   * @throws OutOfMemoryException
   */
  public JSONRecordReader(final FragmentContext fragmentContext, final JsonNode embeddedContent, final DrillFileSystem fileSystem,
      final List<SchemaPath> columns) throws OutOfMemoryException {
    this(fragmentContext, null, embeddedContent, fileSystem, columns);
  }

  private JSONRecordReader(final FragmentContext fragmentContext, final String inputPath, final JsonNode embeddedContent, final DrillFileSystem fileSystem,
                          final List<SchemaPath> columns) throws OutOfMemoryException {

    Preconditions.checkArgument(
        (inputPath == null && embeddedContent != null) ||
        (inputPath != null && embeddedContent == null),
        "One of inputPath or embeddedContent must be set but not both."
        );

    if(inputPath != null){
      this.hadoopPath = new Path(inputPath);
    }else{
      this.embeddedContent = embeddedContent;
    }

    this.fileSystem = fileSystem;
    this.fragmentContext = fragmentContext;

    // only enable all text mode if we aren't using embedded content mode.
    this.enableAllTextMode = embeddedContent == null && fragmentContext.getOptions().getOption(ExecConstants.JSON_READER_ALL_TEXT_MODE_VALIDATOR);
    this.readNumbersAsDouble = fragmentContext.getOptions().getOption(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE).bool_val;
    setColumns(columns);
  }

  @Override
  public void setup(final OperatorContext context, final OutputMutator output) throws ExecutionSetupException {
    this.operatorContext = context;
    try{
      if (hadoopPath != null) {
        this.stream = fileSystem.openPossiblyCompressedStream(hadoopPath);
      }

      this.writer = new VectorContainerWriter(output);
      if (isSkipQuery()) {
        this.jsonReader = new CountingJsonReader(fragmentContext.getManagedBuffer());
      } else {
        this.jsonReader = new JsonReader(fragmentContext.getManagedBuffer(), ImmutableList.copyOf(getColumns()), enableAllTextMode, true, readNumbersAsDouble);
      }
      setupParser();
    }catch(final Exception e){
      handleAndRaise("Failure reading JSON file", e);
    }
  }

  private void setupParser() throws IOException{
    if(hadoopPath != null){
      jsonReader.setSource(stream);
    }else{
      jsonReader.setSource(embeddedContent);
    }
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
    exceptionBuilder.pushContext("Record ", currentRecordNumberInFile())
            .pushContext("File ", hadoopPath.toUri().getPath());

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
    ReadState write = null;
//    Stopwatch p = new Stopwatch().start();
    try{
      outside: while(recordCount < BaseValueVector.INITIAL_VALUE_ALLOCATION){
        writer.setPosition(recordCount);
        write = jsonReader.write(writer);

        if(write == ReadState.WRITE_SUCCEED){
//          logger.debug("Wrote record.");
          recordCount++;
        }else{
//          logger.debug("Exiting.");
          break outside;
        }

      }

      jsonReader.ensureAtLeastOneField(writer);

      writer.setValueCount(recordCount);
//      p.stop();
//      System.out.println(String.format("Wrote %d records in %dms.", recordCount, p.elapsed(TimeUnit.MILLISECONDS)));

      updateRunningCount();

      return recordCount;

    } catch (final Exception e) {
      handleAndRaise("Error parsing JSON", e);
    }
    // this is never reached
    return 0;
  }

  private void updateRunningCount() {
    runningRecordCount += recordCount;
  }

  @Override
  public void cleanup() {
    try {
      if(stream != null){
        stream.close();
      }
    } catch (final IOException e) {
      logger.warn("Failure while closing stream.", e);
    }
  }

}
