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
package org.apache.drill.exec.store.msgpack;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.msgpack.BaseMsgpackReader.ReadState;
import org.apache.drill.exec.store.msgpack.MsgpackFormatPlugin.MsgpackFormatConfig;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.core.JsonParseException;

public class MsgpackRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MsgpackRecordReader.class);

  public static final long DEFAULT_ROWS_PER_BATCH = BaseValueVector.INITIAL_VALUE_ALLOCATION;

  private VectorContainerWriter writer;

  // Data we're consuming
  private Path hadoopPath;
  private InputStream stream;
  private final DrillFileSystem fileSystem;
  private int recordCount;
  private long runningRecordCount = 0;
  private final FragmentContext fragmentContext;
  private long parseErrorCount;
  private final boolean skipMalformedMsgRecords;
  private final boolean printSkippedMalformedMsgRecordLineNumber;
  private ReadState write = null;

  private BaseMsgpackReader messageReader;

  private boolean unionEnabled = false; // ????

  /**
   * Create a msgpack Record Reader that uses a file based input stream.
   *
   * @param fragmentContext
   * @param inputPath
   * @param fileSystem
   * @param columns         pathnames of columns/subfields to read
   * @throws OutOfMemoryException
   */
  public MsgpackRecordReader(MsgpackFormatConfig config, final FragmentContext fragmentContext, final String inputPath,
      final DrillFileSystem fileSystem, final List<SchemaPath> columns) {

    Preconditions.checkArgument((inputPath != null), "InputPath must be set.");
    this.hadoopPath = new Path(inputPath);

    this.fileSystem = fileSystem;
    this.fragmentContext = fragmentContext;
    this.skipMalformedMsgRecords = config.isSkipMalformedMsgRecords();
    this.printSkippedMalformedMsgRecordLineNumber = config.isPrintSkippedMalformedMsgRecordLineNumber();
    setColumns(columns);
  }

  @Override
  public String toString() {
    return super.toString() + "[hadoopPath = " + hadoopPath + ", recordCount = " + recordCount + ", parseErrorCount = "
        + parseErrorCount + ", runningRecordCount = " + runningRecordCount + ", ...]";
  }

  @Override
  public void setup(final OperatorContext context, final OutputMutator output) throws ExecutionSetupException {
    try {
      this.stream = fileSystem.openPossiblyCompressedStream(hadoopPath);

      this.writer = new VectorContainerWriter(output, unionEnabled);
      if (isSkipQuery()) {
        this.messageReader = new CountingMsgpackReader();
      } else {
        this.messageReader = new MsgpackReader(fragmentContext.getManagedBuffer(), Lists.newArrayList(getColumns()));
      }
      setupParser();
    } catch (final Exception e) {
      handleAndRaise("Failure reading mgspack file", e);
    }
  }

  @Override
  protected List<SchemaPath> getDefaultColumnsToRead() {
    return ImmutableList.of();
  }

  private void setupParser() throws IOException {
    messageReader.setSource(stream);
    messageReader.setIgnoreMsgParseErrors(skipMalformedMsgRecords);
  }

  protected void handleAndRaise(String suffix, Exception e) throws UserException {

    String message = e.getMessage();
    int columnNr = -1;

    if (e instanceof JsonParseException) {
      final JsonParseException ex = (JsonParseException) e;
      message = ex.getOriginalMessage();
      columnNr = ex.getLocation().getColumnNr();
    }

    UserException.Builder exceptionBuilder = UserException.dataReadError(e).message("%s - %s", suffix, message);
    if (columnNr > 0) {
      exceptionBuilder.pushContext("Column ", columnNr);
    }

    if (hadoopPath != null) {
      exceptionBuilder.pushContext("Record ", currentRecordNumberInFile()).pushContext("File ",
          hadoopPath.toUri().getPath());
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
    if (write == ReadState.MSG_RECORD_PARSE_EOF_ERROR) {
      return recordCount;
    }
    outside: while (recordCount < DEFAULT_ROWS_PER_BATCH) {
      try {
        writer.setPosition(recordCount);
        write = messageReader.write(writer);
        if (write == ReadState.WRITE_SUCCEED) {
          recordCount++;
        } else if (write == ReadState.MSG_RECORD_PARSE_ERROR || write == ReadState.MSG_RECORD_PARSE_EOF_ERROR) {
          if (skipMalformedMsgRecords == false) {
            handleAndRaise("Error parsing msgpack",
                new Exception(hadoopPath.getName() + " : line nos :" + (recordCount + 1)));
          }
          ++parseErrorCount;
          if (printSkippedMalformedMsgRecordLineNumber) {
            logger.debug(
                "Error parsing msgpack in " + hadoopPath.getName() + " : line nos :" + (recordCount + parseErrorCount));
          }
          if (write == ReadState.MSG_RECORD_PARSE_EOF_ERROR) {
            break outside;
          }
        } else { // END_OF_STREAM
          break outside;
        }
      } catch (IOException ex) {
        handleAndRaise("Error parsing msgpack", ex);
      }
    }
    // Skip empty msgpack file with 0 row.
    // Only when data source has > 0 row, ensure the batch has one field.
    if (recordCount > 0) {
      messageReader.ensureAtLeastOneField(writer);
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
    if (stream != null) {
      stream.close();
    }
  }
}
