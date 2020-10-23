package org.apache.drill.exec.store.pdf;

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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class PdfBatchReader implements ManagedReader<FileScanFramework.FileSchemaNegotiator> {

  private static final Logger logger = LoggerFactory.getLogger(PdfBatchReader.class);
  private static final String NEW_FIELD_PREFIX = "field_";
  private final int maxRecords;
  private final PdfReaderConfig readerConfig;
  private final List<PdfColumnWriter> writers;
  private FileSplit split;
  private CustomErrorContext errorContext;
  private RowSetLoader rowWriter;
  private InputStream fsStream;
  private PDDocument document;
  private PDDocumentInformation info;
  private SchemaBuilder builder;
  private List<String> columnHeaders;


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
  private int rows;
  private int metadataIndex;

  // Tables
  private List<Table> tables;


  static class PdfReaderConfig {
    final PdfFormatPlugin plugin;

    PdfReaderConfig(PdfFormatPlugin plugin) {
      this.plugin = plugin;
    }
  }

  public PdfBatchReader(PdfReaderConfig readerConfig, int maxRecords) {
    this.readerConfig = readerConfig;
    this.maxRecords = maxRecords;
    this.unregisteredColumnCount = 0;
    this.writers = new ArrayList<>();
  }

  @Override
  public boolean open(FileScanFramework.FileSchemaNegotiator negotiator) {
    split = negotiator.split();
    errorContext = negotiator.parentErrorContext();
    builder = new SchemaBuilder();

    openFile(negotiator);
    populateMetadata();

    // Get the tables
    tables = Utils.extractTablesFromPDF(document);
    logger.debug("Found {} tables", tables.size());

    negotiator.tableSchema(buildSchema(), false);
    ResultSetLoader loader = negotiator.build();
    rowWriter = loader.writer();
    buildWriterList();
    addImplicitColumnsToSchema();
    return true;
  }

  @Override
  public boolean next() {
    while(!rowWriter.isFull()) {
      // Check to see if the limit has been reached
      if (rowWriter.limitReached(maxRecords)) {
        return false;
      }
      rowWriter.start();
      Table table = tables.get(0);
      for (List<RectangularTextContainer> row : table.getRows()) {

        // If the dataset unexpectedly adds columns, add to schema
        if (row.size() > columns) {
          // Add column to schema

          // Add writer

          // Increment column counter
          columns++;
        }

        for (int i = 1; i < row.size(); i++) {
          writers.get(i).load(row.get(i));
        }
        rowWriter.save();
      }
    }
    return true;
  }

  @Override
  public void close() {
    if (fsStream != null) {
      AutoCloseables.closeSilently(fsStream);
      fsStream = null;
    }

    if (document != null) {
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
    info = document.getDocumentInformation();
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
  }

  private void addImplicitColumnsToSchema() {
    metadataIndex = columns;
    // Add to schema
    addColumnToSchema("_page_count", MinorType.INT, true);
    addColumnToSchema("_title", MinorType.VARCHAR, true);
    addColumnToSchema("_author", MinorType.VARCHAR, true);
    addColumnToSchema("_subject", MinorType.VARCHAR, true);
    addColumnToSchema("_keywords", MinorType.VARCHAR, true);
    addColumnToSchema("_creator", MinorType.VARCHAR, true);
    addColumnToSchema("_producer", MinorType.VARCHAR, true);
    addColumnToSchema("_creationDate", MinorType.TIMESTAMP, true);
    addColumnToSchema("_modificationDate", MinorType.TIMESTAMP, true);
    addColumnToSchema("_trapped", MinorType.VARCHAR, true);
  }

  private void addColumnToSchema(String columnName, MinorType dataType, boolean isMetadata) {
    int index = rowWriter.tupleSchema().index(columnName);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(columnName, dataType, TypeProtos.DataMode.OPTIONAL);
      if (isMetadata) {
        colSchema.setBooleanProperty(ColumnMetadata.EXCLUDE_FROM_WILDCARD, true);
        metadataIndex++;
      }
      index = rowWriter.addColumn(colSchema);
    }

    writers.add(new StringPdfColumnWriter(index, columnName, rowWriter));
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
    unregisteredColumnCount ++;

    // Add a new writer.  Since we want the metadata always to be at the end of the schema, we must track the metadata
    // index and add this before the metadata, so that the column index tracks with the writer index.
    writers.add(metadataIndex, new StringPdfColumnWriter(1, newColumnName, (RowSetLoader) rowWriter));

    // Increment the metadata index
    metadataIndex++;
  }

  private TupleMetadata buildSchema() {
    Table table = tables.get(0); // TODO... cases where there are more than one table
    columns = table.getColCount();
    rows = table.getRowCount();

    // Get column header names
    columnHeaders = Utils.extractRowValues(table);

    // Add columns to table
    for (String columnName : columnHeaders) {
      builder.addNullable(columnName, MinorType.VARCHAR);
    }

    return builder.buildSchema();
  }

  private void buildWriterList() {
    for (String header : columnHeaders) {
      writers.add(new StringPdfColumnWriter(columnHeaders.indexOf(header), header, rowWriter));
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
