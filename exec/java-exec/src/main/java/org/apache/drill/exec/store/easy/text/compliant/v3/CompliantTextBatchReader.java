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
package org.apache.drill.exec.store.easy.text.compliant.v3;

import java.io.IOException;
import java.io.InputStream;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsArrayManager;
import org.apache.drill.exec.physical.impl.scan.columns.ColumnsSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.hadoop.mapred.FileSplit;

import com.univocity.parsers.common.TextParsingException;

import io.netty.buffer.DrillBuf;

/**
 * New text reader, complies with the RFC 4180 standard for text/csv files
 */
public class CompliantTextBatchReader implements ManagedReader<ColumnsSchemaNegotiator> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CompliantTextBatchReader.class);

  private static final int MAX_RECORDS_PER_BATCH = 8096;
  private static final int READ_BUFFER = 1024 * 1024;
  private static final int WHITE_SPACE_BUFFER = 64 * 1024;

  // settings to be used while parsing
  private final TextParsingSettingsV3 settings;
  // Chunk of the file to be read by this reader
  private final FileSplit split;
  // text reader implementation
  private TextReader reader;
  // input buffer
  private DrillBuf readBuffer;
  // working buffer to handle whitespaces
  private DrillBuf whitespaceBuffer;
  private final DrillFileSystem dfs;

  private RowSetLoader writer;

  public CompliantTextBatchReader(FileSplit split, DrillFileSystem dfs, TextParsingSettingsV3 settings) {
    this.split = split;
    this.settings = settings;
    this.dfs = dfs;

    // Validate. Otherwise, these problems show up later as a data
    // read error which is very confusing.

    if (settings.getNewLineDelimiter().length == 0) {
      throw UserException
        .validationError()
        .message("The text format line delimiter cannot be blank.")
        .build(logger);
    }
  }

  /**
   * Performs the initial setup required for the record reader.
   * Initializes the input stream, handling of the output record batch
   * and the actual reader to be used.
   * @param context  operator context from which buffer's will be allocated and managed
   * @param outputMutator  Used to create the schema in the output record batch
   */

  @Override
  public boolean open(ColumnsSchemaNegotiator schemaNegotiator) {
    final OperatorContext context = schemaNegotiator.context();

    // Note: DO NOT use managed buffers here. They remain in existence
    // until the fragment is shut down. The buffers here are large.
    // If we scan 1000 files, and allocate 1 MB for each, we end up
    // holding onto 1 GB of memory in managed buffers.
    // Instead, we allocate the buffers explicitly, and must free
    // them.

    readBuffer = context.getAllocator().buffer(READ_BUFFER);
    whitespaceBuffer = context.getAllocator().buffer(WHITE_SPACE_BUFFER);

    // TODO: Set this based on size of record rather than
    // absolute count.

    schemaNegotiator.setBatchSize(MAX_RECORDS_PER_BATCH);

    // setup Output, Input, and Reader
    try {
      TextOutput output;

      if (settings.isHeaderExtractionEnabled()) {
        output = openWithHeaders(schemaNegotiator);
      } else {
        output = openWithoutHeaders(schemaNegotiator);
      }
      if (output == null) {
        return false;
      }
      openReader(output);
      return true;
    } catch (final IOException e) {
      throw UserException.dataReadError(e).addContext("File Path", split.getPath().toString()).build(logger);
    }
  }

  /**
   * Extract header and use that to define the reader schema.
   */

  private TextOutput openWithHeaders(ColumnsSchemaNegotiator schemaNegotiator) throws IOException {
    final String [] fieldNames = extractHeader();
    if (fieldNames == null) {
      return null;
    }
    final TupleMetadata schema = new TupleSchema();
    for (final String colName : fieldNames) {
      schema.addColumn(MetadataUtils.newScalar(colName, MinorType.VARCHAR, DataMode.REQUIRED));
    }
    schemaNegotiator.setTableSchema(schema, true);
    writer = schemaNegotiator.build().writer();
    return new FieldVarCharOutput(writer);
  }

  /**
   * When no headers, create a single array column "columns".
   */

  private TextOutput openWithoutHeaders(
      ColumnsSchemaNegotiator schemaNegotiator) {
    final TupleMetadata schema = new TupleSchema();
    schema.addColumn(MetadataUtils.newScalar(ColumnsArrayManager.COLUMNS_COL, MinorType.VARCHAR, DataMode.REPEATED));
    schemaNegotiator.setTableSchema(schema, true);
    writer = schemaNegotiator.build().writer();
    return new RepeatedVarCharOutput(writer, schemaNegotiator.projectedIndexes());
  }

  private void openReader(TextOutput output) throws IOException {
    logger.trace("Opening file {}", split.getPath());
    final InputStream stream = dfs.openPossiblyCompressedStream(split.getPath());
    final TextInput input = new TextInput(settings, stream, readBuffer,
        split.getStart(), split.getStart() + split.getLength());

    // setup Reader using Input and Output
    reader = new TextReader(settings, input, output, whitespaceBuffer);
    reader.start();
  }

  /**
   * Extracts header from text file.
   * Currently it is assumed to be first line if headerExtractionEnabled is set to true
   * TODO: enhance to support more common header patterns
   * @return field name strings
   */

  private String [] extractHeader() throws IOException {
    assert settings.isHeaderExtractionEnabled();

    // don't skip header in case skipFirstLine is set true
    settings.setSkipFirstLine(false);

    final HeaderBuilder hOutput = new HeaderBuilder(split.getPath());

    // setup Input using InputStream
    // we should read file header irrespective of split given given to this reader
    final InputStream hStream = dfs.openPossiblyCompressedStream(split.getPath());
    final TextInput hInput = new TextInput(settings, hStream, readBuffer, 0, split.getLength());

    // setup Reader using Input and Output
    this.reader = new TextReader(settings, hInput, hOutput, whitespaceBuffer);
    reader.start();

    // extract first row only
    reader.parseNext();

    // grab the field names from output
    final String [] fieldNames = hOutput.getHeaders();

    // cleanup and set to skip the first line next time we read input
    reader.close();
    settings.setSkipFirstLine(true);

    readBuffer.clear();
    whitespaceBuffer.clear();
    return fieldNames;
  }

  /**
   * Generates the next record batch
   * @return  number of records in the batch
   */

  @Override
  public boolean next() {
    reader.resetForNextBatch();

    try {
      boolean more = false;
      while (! writer.isFull()) {
        more = reader.parseNext();
        if (! more) {
          break;
        }
      }
      reader.finishBatch();

      // Return false on the batch that hits EOF. The scan operator
      // knows to process any rows in this final batch.

      return more && writer.rowCount() > 0;
    } catch (IOException | TextParsingException e) {
      if (e.getCause() != null  && e.getCause() instanceof UserException) {
        throw (UserException) e.getCause();
      }
      throw UserException.dataReadError(e)
          .addContext("Failure while reading file %s. Happened at or shortly before byte position %d.",
            split.getPath(), reader.getPos())
          .build(logger);
    }
  }

  /**
   * Cleanup state once we are finished processing all the records.
   * This would internally close the input stream we are reading from.
   */
  @Override
  public void close() {

    // Release the buffers allocated above. Double-check to handle
    // unexpected multiple calls to close().

    if (readBuffer != null) {
      readBuffer.release();
      readBuffer = null;
    }
    if (whitespaceBuffer != null) {
      whitespaceBuffer.release();
      whitespaceBuffer = null;
    }
    try {
      if (reader != null) {
        reader.close();
        reader = null;
      }
    } catch (final IOException e) {
      logger.warn("Exception while closing stream.", e);
    }
  }
}
