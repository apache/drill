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

package org.apache.drill.exec.store.pdf;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.shaded.guava.com.google.common.base.Strings;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import technology.tabula.RectangularTextContainer;
import technology.tabula.Table;

import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class PdfBatchReader implements ManagedReader<FileScanFramework.FileSchemaNegotiator> {

  private static final Logger logger = LoggerFactory.getLogger(PdfBatchReader.class);
  private static final String NEW_FIELD_PREFIX = "field_";
  private final int maxRecords;

  private final List<PdfColumnWriter> writers;
  private FileSplit split;
  private CustomErrorContext errorContext;
  private RowSetLoader rowWriter;
  private InputStream fsStream;
  private PDDocument document;
  private PdfReaderConfig config;

  private SchemaBuilder builder;
  private List<String> columnHeaders;
  private int currentRowIndex;
  private Table currentTable;


  // Document Metadata Fields
  private int pageCount;
  private String title;
  private String author;
  private String subject;
  private String keywords;
  private String creator;
  private String producer;
  private Calendar creationDate;
  private Calendar modificationDate;
  private String trapped;
  private int unregisteredColumnCount;
  private int columns;
  private int tableCount;
  private int rows;
  private int metadataIndex;
  private int currentTableIndex;

  // Tables
  private List<Table> tables;


  static class PdfReaderConfig {
    final PdfFormatPlugin plugin;

    PdfReaderConfig(PdfFormatPlugin plugin) {
      this.plugin = plugin;
    }
  }

  public PdfBatchReader(PdfReaderConfig readerConfig, int maxRecords) {
    this.maxRecords = maxRecords;
    this.unregisteredColumnCount = 0;
    this.currentTableIndex = 0;
    this.writers = new ArrayList<>();
    this.config = readerConfig;
  }

  @Override
  public boolean open(FileScanFramework.FileSchemaNegotiator negotiator) {
    System.setProperty("java.awt.headless", "true");

    split = negotiator.split();
    errorContext = negotiator.parentErrorContext();
    builder = new SchemaBuilder();

    openFile(negotiator);

    // Get the tables
    tables = Utils.extractTablesFromPDF(document);
    populateMetadata();
    logger.debug("Found {} tables", tables.size());

    // Support provided schema
    TupleMetadata schema = null;
    if (negotiator.hasProvidedSchema()) {
      schema = negotiator.providedSchema();
      negotiator.tableSchema(schema, false);
    } else {
      negotiator.tableSchema(buildSchema(), false);
    }
    ResultSetLoader loader = negotiator.build();
    rowWriter = loader.writer();

    if (negotiator.hasProvidedSchema()) {
      buildWriterListFromProvidedSchema(schema);
    } else {
      buildWriterList();
    }
    addImplicitColumnsToSchema();

    // Prepare for reading
    currentRowIndex = 1;  // Skip the first line if there are headers
    currentTable = tables.get(0);

    return true;
  }

  @Override
  public boolean next() {
    System.setProperty("java.awt.headless", "true");

    while(!rowWriter.isFull()) {
      // Check to see if the limit has been reached
      if (rowWriter.limitReached(maxRecords)) {
        return false;
      } else if (currentRowIndex >= currentTable.getRows().size() &&
                  currentTableIndex < tables.size() &&
                  config.plugin.getConfig().getCombinePages()) {
        logger.debug("Merging table {} with current table.", currentTableIndex);
        currentRowIndex = 0;
        currentTable = tables.get(currentTableIndex++);
      } else if (currentRowIndex >= currentTable.getRows().size()) {
        return false;
      }

      // Process the row
      processRow(currentTable.getRows().get(currentRowIndex));
      currentRowIndex++;
    }
    return true;
  }

  private void processRow(List<RectangularTextContainer> row) {
    String value;
    rowWriter.start();
    for (int i = 0; i < row.size(); i++) {
      value = row.get(i).getText();

      if (Strings.isNullOrEmpty(value)) {
        continue;
      }
      writers.get(i).load(row.get(i));
    }
    writeMetadata();
    rowWriter.save();
  }

  @Override
  public void close() {
    if (fsStream != null) {
      AutoCloseables.closeSilently(fsStream);
      fsStream = null;
    }

    if (document != null) {
      AutoCloseables.closeSilently(document.getDocument());
      AutoCloseables.closeSilently(document);
      document = null;
    }
  }

  /**
   * This method opens the PDF file, and finds the tables
   * @param negotiator The Drill file negotiator object that represents the file system
   */
  private void openFile(FileScanFramework.FileSchemaNegotiator negotiator) {
    try {
      fsStream = negotiator.fileSystem().openPossiblyCompressedStream(split.getPath());
      document = PDDocument.load(fsStream);
    } catch (Exception e) {
      throw UserException
        .dataReadError(e)
        .message("Failed to open open input file: %s", split.getPath().toString())
        .addContext(e.getMessage())
        .addContext(errorContext)
        .build(logger);
    }
  }

  /**
   * Metadata fields are calculated once when the file is opened.  This function populates
   * the metadata fields so that these are only calculated once.
   */
  private void populateMetadata() {
    PDDocumentInformation info = document.getDocumentInformation();
    pageCount = document.getNumberOfPages();
    title = info.getTitle();
    author = info.getAuthor();
    subject = info.getSubject();
    keywords = info.getKeywords();
    creator = info.getCreator();
    producer = info.getProducer();
    creationDate = info.getCreationDate();
    modificationDate = info.getModificationDate();
    trapped = info.getTrapped();
    tableCount = tables.size();
  }

  private void addImplicitColumnsToSchema() {
    metadataIndex = columns;
    // Add to schema
    addMetadataColumnToSchema("_page_count", MinorType.INT);
    addMetadataColumnToSchema("_title", MinorType.VARCHAR);
    addMetadataColumnToSchema("_author", MinorType.VARCHAR);
    addMetadataColumnToSchema("_subject", MinorType.VARCHAR);
    addMetadataColumnToSchema("_keywords", MinorType.VARCHAR);
    addMetadataColumnToSchema("_creator", MinorType.VARCHAR);
    addMetadataColumnToSchema("_producer", MinorType.VARCHAR);
    addMetadataColumnToSchema("_creation_date", MinorType.TIMESTAMP);
    addMetadataColumnToSchema("_modification_date", MinorType.TIMESTAMP);
    addMetadataColumnToSchema("_trapped", MinorType.VARCHAR);
    addMetadataColumnToSchema("_table_count", MinorType.INT);
  }

  private void addMetadataColumnToSchema(String columnName, MinorType dataType) {
    int index = rowWriter.tupleSchema().index(columnName);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(columnName, dataType, DataMode.OPTIONAL);

      // Exclude from wildcard queries
      colSchema.setBooleanProperty(ColumnMetadata.EXCLUDE_FROM_WILDCARD, true);
      metadataIndex++;

      index = rowWriter.addColumn(colSchema);
    }

    writers.add(new StringPdfColumnWriter(index, columnName, rowWriter));
  }

  private void writeMetadata() {
    int startingIndex = columnHeaders.size();
    writers.get(startingIndex).getWriter().setInt(pageCount);
    writeStringMetadataField(title, startingIndex+1);
    writeStringMetadataField(author, startingIndex+2);
    writeStringMetadataField(subject, startingIndex+3);
    writeStringMetadataField(keywords, startingIndex+4);
    writeStringMetadataField(creator, startingIndex+5);
    writeStringMetadataField(producer, startingIndex+6);
    writeTimestampMetadataField(creationDate, startingIndex+7);
    writeTimestampMetadataField(modificationDate, startingIndex+8);
    writeStringMetadataField(trapped, startingIndex+9);
    writers.get(startingIndex+10).getWriter().setInt(tableCount);
  }

  private void writeStringMetadataField(String value, int index) {
    if (value == null) {
      return;
    }
    writers.get(index).getWriter().setString(value);
  }

  private void writeTimestampMetadataField(Calendar dateValue, int index) {
    if (dateValue == null) {
      return;
    }

    writers.get(index).getWriter().setTimestamp(Instant.ofEpochMilli(dateValue.getTimeInMillis()));
  }

  private void addUnknownColumnToSchemaAndCreateWriter (TupleWriter rowWriter, String name) {
    int index = rowWriter.tupleSchema().index(name);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(name, TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL);
      index = rowWriter.addColumn(colSchema);
    }
    ScalarWriter colWriter = rowWriter.scalar(index);

    // Create a new column name which will be field_n
    String newColumnName = NEW_FIELD_PREFIX + unregisteredColumnCount;
    unregisteredColumnCount++;

    // Add a new writer.  Since we want the metadata always to be at the end of the schema, we must track the metadata
    // index and add this before the metadata, so that the column index tracks with the writer index.
    writers.add(metadataIndex, new StringPdfColumnWriter(1, newColumnName, (RowSetLoader) rowWriter));

    // Increment the metadata index
    metadataIndex++;
  }

  private TupleMetadata buildSchema() {
    Table table = tables.get(0);
    columns = table.getColCount();
    rows = table.getRowCount();

    // Get column header names
    columnHeaders = Utils.extractRowValues(table);

    // Add columns to table
    int index = 0;
    for (String columnName : columnHeaders) {
      if (Strings.isNullOrEmpty(columnName)) {
        columnName = NEW_FIELD_PREFIX + unregisteredColumnCount;
        columnHeaders.set(index, columnName);
        unregisteredColumnCount++;
      }
      builder.addNullable(columnName, MinorType.VARCHAR);
      index++;
    }

    return builder.buildSchema();
  }

  private void buildWriterList() {
    for (String header : columnHeaders) {
      writers.add(new StringPdfColumnWriter(columnHeaders.indexOf(header), header, rowWriter));
    }
  }

  private void buildWriterListFromProvidedSchema(TupleMetadata schema) {
    if (schema == null) {
      buildWriterList();
      return;
    }
    int counter = 0;
    for (MaterializedField field: schema.toFieldList()) {
      String fieldName = field.getName();
      MinorType type = field.getType().getMinorType();
      columnHeaders.add(fieldName);

      switch (type) {
        case VARCHAR:
          writers.add(new StringPdfColumnWriter(counter, fieldName, rowWriter));
          break;
        case SMALLINT:
        case TINYINT:
        case INT:
          writers.add(new IntPdfColumnWriter(counter, fieldName, rowWriter));
          break;
        case BIGINT:
          writers.add(new BigIntPdfColumnWriter(counter, fieldName, rowWriter));
          break;
        case FLOAT4:
        case FLOAT8:
          writers.add(new DoublePdfColumnWriter(counter, fieldName, rowWriter));
          break;
        default:
          throw UserException.unsupportedError()
            .message("PDF Reader with Provided Schema only supports String, and Numeric Types")
            .addContext(errorContext)
            .build(logger);
      }
    }
  }

  public abstract static class PdfColumnWriter {
    final String columnName;
    final ScalarWriter writer;
    final int columnIndex;

    public PdfColumnWriter(int columnIndex, String columnName, ScalarWriter writer) {
      this.columnIndex = columnIndex;
      this.columnName = columnName;
      this.writer = writer;
    }

    public abstract void load (RectangularTextContainer<?> cell);

    public ScalarWriter getWriter() {
      return writer;
    }
  }

  public static class IntPdfColumnWriter extends PdfColumnWriter {
    IntPdfColumnWriter (int columnIndex, String columnName, RowSetLoader rowWriter) {
      super(columnIndex, columnName, rowWriter.scalar(columnName));
    }

    @Override
    public void load(RectangularTextContainer<?> cell) {
      writer.setInt(Integer.parseInt(cell.getText()));
    }
  }

  public static class BigIntPdfColumnWriter extends PdfColumnWriter {
    BigIntPdfColumnWriter (int columnIndex, String columnName, RowSetLoader rowWriter) {
      super(columnIndex, columnName, rowWriter.scalar(columnName));
    }

    @Override
    public void load(RectangularTextContainer<?> cell) {
      writer.setLong(Long.parseLong(cell.getText()));
    }
  }

  public static class DoublePdfColumnWriter extends PdfColumnWriter {
    DoublePdfColumnWriter (int columnIndex, String columnName, RowSetLoader rowWriter) {
      super(columnIndex, columnName, rowWriter.scalar(columnName));
    }

    @Override
    public void load(RectangularTextContainer<?> cell) {
      writer.setDouble(Double.parseDouble(cell.getText()));
    }
  }

  public static class StringPdfColumnWriter extends PdfColumnWriter {
    StringPdfColumnWriter (int columnIndex, String columnName, RowSetLoader rowWriter) {
      super(columnIndex, columnName, rowWriter.scalar(columnName));
    }

    @Override
    public void load(RectangularTextContainer<?> cell) {
      writer.setString(cell.getText());
    }
  }
}
