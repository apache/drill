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
package org.apache.drill.exec.store.easy.text.reader;

import java.io.IOException;
import java.io.InputStream;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.scan.v3.FixedReceiver;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.v3.schema.ProjectedColumn;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.exec.store.easy.text.TextFormatPlugin;
import org.apache.drill.exec.vector.accessor.ValueWriter;
import org.apache.hadoop.mapred.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.univocity.parsers.common.TextParsingException;

import io.netty.buffer.DrillBuf;

/**
 * Text reader, Complies with the RFC 4180 standard for text/csv files.
 */
public class CompliantTextBatchReader implements ManagedReader {
  private static final Logger logger = LoggerFactory.getLogger(CompliantTextBatchReader.class);

  public static final String COLUMNS_COL = "columns";
  private static final int MAX_RECORDS_PER_BATCH = 8096;
  private static final int READ_BUFFER = 1024 * 1024;
  private static final int WHITE_SPACE_BUFFER = 64 * 1024;

  // settings to be used while parsing
  private final TextParsingSettings settings;
  private final CustomErrorContext errorContext;
  // text reader implementation
  private final TextReader reader;
  // input buffer
  private final DrillBuf readBuffer;
  // working buffer to handle whitespace
  private final DrillBuf whitespaceBuffer;

  private RowSetLoader writer;

  /**
   * Create and open the text reader.
   * @throws EarlyEofException
   */
  public CompliantTextBatchReader(FileSchemaNegotiator schemaNegotiator,
      TextParsingSettings settings) throws EarlyEofException {
    this.settings = settings;
    this.errorContext = schemaNegotiator.parentErrorContext();

    // Validate. Otherwise, these problems show up later as a data
    // read error which is very confusing.
    if (settings.getNewLineDelimiter().length == 0) {
      throw UserException
        .validationError()
        .message("The text format line delimiter cannot be blank.")
        .addContext(errorContext)
        .build(logger);
    }

    final OperatorContext context = schemaNegotiator.context();

    // Note: DO NOT use managed buffers here. They remain in existence
    // until the fragment is shut down. The buffers here are large.
    // If we scan 1000 files, and allocate 1 MB for each, we end up
    // holding onto 1 GB of memory in managed buffers.
    // Instead, we allocate the buffers explicitly, and must free
    // them.
    this.readBuffer = context.getAllocator().buffer(READ_BUFFER);
    this.whitespaceBuffer = context.getAllocator().buffer(WHITE_SPACE_BUFFER);
    schemaNegotiator.batchSize(MAX_RECORDS_PER_BATCH);

    // setup Output, Input, and Reader
    try {
      TextOutput output;
      if (settings.isHeaderExtractionEnabled()) {
        output = openWithHeaders(schemaNegotiator);
      } else {
        output = openWithoutHeaders(schemaNegotiator);
      }
      if (output == null) {
        throw new EarlyEofException();
      }
      this.reader = openReader(schemaNegotiator, output);
    } catch (final IOException e) {
      throw UserException.dataReadError(e)
        .addContext("File open failed")
        .addContext(errorContext)
        .build(logger);
    }
  }

  /**
   * Extract header and use that to define the reader schema.
   *
   * @param schemaNegotiator used to define the reader schema
   */
  private TextOutput openWithHeaders(FileSchemaNegotiator schemaNegotiator) throws IOException {
    validateNoColumnsProjection(schemaNegotiator);
    final String [] fieldNames = extractHeader(schemaNegotiator);
    if (fieldNames == null) {
      return null;
    }
    if (schemaNegotiator.providedSchema() != null) {
      return buildWithSchema(schemaNegotiator, fieldNames);
    } else {
      return buildFromColumnHeaders(schemaNegotiator, fieldNames);
    }
  }

  /**
   * File has headers and a provided schema is provided. Convert from VARCHAR
   * input type to the provided output type, but only if the column is projected.
   */
  private FieldVarCharOutput buildWithSchema(FileSchemaNegotiator schemaNegotiator,
      String[] fieldNames) {

    TupleMetadata readerSchema = buildSchemaFromHeaders(fieldNames);

    // Build converting column writers
    FixedReceiver.Builder builder = FixedReceiver.builderFor(schemaNegotiator)
        .schemaIsComplete();
    builder.conversionBuilder().blankAs(ColumnMetadata.BLANK_AS_NULL);
    FixedReceiver receiver = builder.build(readerSchema);
    writer = receiver.rowWriter();
    return new FieldVarCharOutput(receiver);
  }

  private TupleMetadata buildSchemaFromHeaders(String[] fieldNames) {
    // Build table schema from headers
    TupleMetadata readerSchema = new TupleSchema();
    for (String name : fieldNames) {
      readerSchema.addColumn(textColumn(name));
    }
    return readerSchema;
  }

  private ColumnMetadata textColumn(String colName) {
    return MetadataUtils.newScalar(colName, MinorType.VARCHAR, DataMode.REQUIRED);
  }

  /**
   * File has column headers. No provided schema. Build schema from the
   * column headers.
   */
  private FieldVarCharOutput buildFromColumnHeaders(FileSchemaNegotiator schemaNegotiator,
      String[] fieldNames) {
    TupleMetadata readerSchema = buildSchemaFromHeaders(fieldNames);
    schemaNegotiator.tableSchema(readerSchema, true);
    writer = schemaNegotiator.build().writer();
    ValueWriter[] colWriters = new ValueWriter[fieldNames.length];
    for (int i = 0; i < fieldNames.length; i++) {
      colWriters[i] = writer.column(i).scalar();
    }
    return new FieldVarCharOutput(writer, colWriters);
  }

  /**
   * When no headers, create a single array column "columns".
   */
  private TextOutput openWithoutHeaders(
      FileSchemaNegotiator schemaNegotiator) {
    // Treat a property-only schema as no schema
    TupleMetadata providedSchema = schemaNegotiator.providedSchema();
    if (providedSchema != null && providedSchema.size() > 0) {
      return buildWithSchema(schemaNegotiator);
    } else {
      return buildColumnsArray(schemaNegotiator);
    }
  }

  private FieldVarCharOutput buildWithSchema(FileSchemaNegotiator schemaNegotiator) {
    validateNoColumnsProjection(schemaNegotiator);

    // Build table schema from provided
    TupleMetadata readerSchema = new TupleSchema();
    for (ColumnMetadata providedCol : schemaNegotiator.providedSchema()) {
      readerSchema.addColumn(textColumn(providedCol.name()));
    }

    // Build converting column writers
    FixedReceiver.Builder builder = FixedReceiver.builderFor(schemaNegotiator)
        .schemaIsComplete();
    builder.conversionBuilder().blankAs(ColumnMetadata.BLANK_AS_NULL);
    FixedReceiver receiver = builder.build(readerSchema);

    // Convert to format for this reader
    writer = receiver.rowWriter();
    return new ConstrainedFieldOutput(receiver);
  }

  private void validateNoColumnsProjection(FileSchemaNegotiator schemaNegotiator) {
    // If we do not require the columns array, then we presume that
    // the reader does not provide arrays, so any use of the columns[x]
    // column is likely an error. We rely on the plugin's own error
    // context to fill in information that would explain the issue
    // in the context of that plugin.

    ProjectedColumn colProj = schemaNegotiator.projectionFor(COLUMNS_COL);
    if (colProj != null && colProj.isArray()) {
      throw UserException
          .validationError()
          .message("Unexpected `columns`[x]; file has headers or schema")
          .addContext(errorContext)
          .build(logger);
    }
  }

  private TextOutput buildColumnsArray(
      FileSchemaNegotiator schemaNegotiator) {
    ProjectedColumn colProj = schemaNegotiator.projectionFor(COLUMNS_COL);
    validateColumnsProjection(colProj);
    schemaNegotiator.tableSchema(columnsSchema(), true);
    writer = schemaNegotiator.build().writer();
    return new RepeatedVarCharOutput(writer,
        colProj == null ? null : colProj.indexes());
  }

  private void validateColumnsProjection(ProjectedColumn colProj) {
    if (colProj == null) {
      return;
    }

    // The columns column cannot be a map. That is, the following is
    // not allowed: columns.foo.

    if (colProj.isMap()) {
      throw UserException
        .validationError()
        .message("Column `%s` has map elements, but must be an array", colProj.name())
        .addContext(errorContext)
        .build(logger);
    }

    if (colProj.isArray()) {
      int maxIndex = colProj.maxIndex();
      if (maxIndex > TextFormatPlugin.MAXIMUM_NUMBER_COLUMNS) {
        throw UserException
          .validationError()
          .message("`columns`[%d] index out of bounds, max supported size is %d",
              maxIndex, TextFormatPlugin.MAXIMUM_NUMBER_COLUMNS)
          .addContext("Column:", colProj.name())
          .addContext("Maximum index:", TextFormatPlugin.MAXIMUM_NUMBER_COLUMNS)
          .addContext("Actual index:", maxIndex)
          .addContext(errorContext)
          .build(logger);
      }
    }
  }

  private TextReader openReader(FileSchemaNegotiator schemaNegotiator, TextOutput output) throws IOException {
    FileSplit split = schemaNegotiator.file().split();
    logger.trace("Opening file {}", split.getPath());
    final InputStream stream = schemaNegotiator.file().open();
    final TextInput input = new TextInput(settings, stream, readBuffer,
        split.getStart(), split.getStart() + split.getLength());

    // setup Reader using Input and Output
    TextReader reader = new TextReader(settings, input, output, whitespaceBuffer);
    reader.start();
    return reader;
  }

  public static TupleMetadata columnsSchema() {
    return new SchemaBuilder()
      .addArray(COLUMNS_COL, MinorType.VARCHAR)
      .buildSchema();
  }

  /**
   * Extracts header from text file.
   * Currently it is assumed to be first line if headerExtractionEnabled is set to true
   * TODO: enhance to support more common header patterns
   * @return field name strings
   */
  private String[] extractHeader(FileSchemaNegotiator schemaNegotiator) throws IOException {
    assert settings.isHeaderExtractionEnabled();

    // don't skip header in case skipFirstLine is set true
    settings.setSkipFirstLine(false);

    FileSplit split = schemaNegotiator.file().split();
    logger.trace("Opening file {}", split.getPath());
    final InputStream hStream = schemaNegotiator.file().open();
    final HeaderBuilder hOutput = new HeaderBuilder(split.getPath());

    // we should read file header irrespective of split given given to this reader
    final TextInput hInput = new TextInput(settings, hStream, readBuffer, 0, split.getLength());

    final String [] fieldNames;
    try (TextReader reader = new TextReader(settings, hInput, hOutput, whitespaceBuffer)) {
      reader.start();

      // extract first row only
      reader.parseNext();

      // grab the field names from output
      fieldNames = hOutput.getHeaders();
    }

    settings.setSkipFirstLine(true);

    readBuffer.clear();
    whitespaceBuffer.clear();
    return fieldNames;
  }

  /**
   * Generates the next record batch
   * @return number of records in the batch
   */
  @Override
  public boolean next() {
    reader.resetForNextBatch();

    try {
      boolean more = false;
      while (!writer.isFull()) {
        more = reader.parseNext();
        if (! more) {
          break;
        }
      }
      reader.finishBatch();

      // Return false on the batch that hits EOF. The scan operator
      // knows to process any rows in this final batch.

      return more;
    } catch (IOException | TextParsingException e) {
      if (e.getCause() != null  && e.getCause() instanceof UserException) {
        throw (UserException) e.getCause();
      }
      throw UserException.dataReadError(e)
          .addContext("Failure while reading file")
          .addContext("Happened at or shortly before byte position", reader.getPos())
          .addContext(errorContext)
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
    readBuffer.release();
    whitespaceBuffer.release();
    AutoCloseables.closeSilently(reader);
  }
}
