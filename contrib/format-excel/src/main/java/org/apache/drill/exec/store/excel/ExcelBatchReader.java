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

package org.apache.drill.exec.store.excel;

import com.github.pjfanning.xlsx.StreamingReader;
import com.github.pjfanning.xlsx.impl.StreamingWorkbook;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.poi.ooxml.POIXMLProperties.CoreProperties;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TimeZone;

public class ExcelBatchReader implements ManagedReader<FileSchemaNegotiator> {

  private static final Logger logger = LoggerFactory.getLogger(ExcelBatchReader.class);

  private static final String SAFE_WILDCARD = "_$";
  private static final String SAFE_SEPARATOR = "_";
  private static final String PARSER_WILDCARD = ".*";
  private static final String HEADER_NEW_LINE_REPLACEMENT = "__";
  private static final String MISSING_FIELD_NAME_HEADER = "field_";

  private enum IMPLICIT_STRING_COLUMN {
    CATEGORY("_category"),
    CONTENT_STATUS("_content_status"),
    CONTENT_TYPE("_content_type"),
    CREATOR("_creator"),
    DESCRIPTION("_description"),
    IDENTIFIER("_identifier"),
    KEYWORDS("_keywords"),
    LAST_MODIFIED_BY_USER("_last_modified_by_user"),
    REVISION("_revision"),
    SUBJECT("_subject"),
    TITLE("_title");

    private final String fieldName;

    IMPLICIT_STRING_COLUMN(String fieldName) {
      this.fieldName = fieldName;
    }

    public String getFieldName() {
      return fieldName;
    }
  }

  private enum IMPLICIT_TIMESTAMP_COLUMN {
    /**
     * The file created date
     */
    CREATED("_created"),
    /**
     * Date the file was last printed, null if never printed.
     */
    LAST_PRINTED("_last_printed"),
    /**
     * Date of last modification
     */
    MODIFIED("_modified");

    private final String fieldName;

    IMPLICIT_TIMESTAMP_COLUMN(String fieldName) {
      this.fieldName = fieldName;
    }

    public String getFieldName() {
      return fieldName;
    }
  }

  private static final int ROW_CACHE_SIZE = 100;
  private static final int BUFFER_SIZE = 4096;

  private final ExcelReaderConfig readerConfig;
  private Sheet sheet;
  private Row currentRow;
  private StreamingWorkbook streamingWorkbook;
  private InputStream fsStream;
  private List<String> excelFieldNames;
  private List<ScalarWriter> columnWriters;
  private List<CellWriter> cellWriterArray;
  private List<ScalarWriter> metadataColumnWriters;
  private Iterator<Row> rowIterator;
  private RowSetLoader rowWriter;
  private int totalColumnCount;
  private boolean firstLine;
  private FileSplit split;
  private int recordCount;
  private Map<String, String> stringMetadata;
  private Map<String, Date> dateMetadata;
  private CustomErrorContext errorContext;


  static class ExcelReaderConfig {
    final ExcelFormatPlugin plugin;
    final int headerRow;
    final int lastRow;
    final int firstColumn;
    final int lastColumn;
    final boolean allTextMode;
    final String sheetName;

    ExcelReaderConfig(ExcelFormatPlugin plugin) {
      this.plugin = plugin;
      headerRow = plugin.getConfig().getHeaderRow();
      lastRow = plugin.getConfig().getLastRow();
      firstColumn = plugin.getConfig().getFirstColumn();
      lastColumn = plugin.getConfig().getLastColumn();
      allTextMode = plugin.getConfig().getAllTextMode();
      sheetName = plugin.getConfig().getSheetName();
    }
  }

  public ExcelBatchReader(ExcelReaderConfig readerConfig) {
    this.readerConfig = readerConfig;
    firstLine = true;
  }

  @Override
  public boolean open(FileSchemaNegotiator negotiator) {
    split = negotiator.split();
    errorContext = negotiator.parentErrorContext();
    ResultSetLoader loader = negotiator.build();
    rowWriter = loader.writer();
    openFile(negotiator);
    defineSchema();
    return true;
  }

  /**
   * This method opens the Excel file, initializes the Streaming Excel Reader, and initializes the sheet variable.
   * @param negotiator The Drill file negotiator object that represents the file system
   */
  private void openFile(FileScanFramework.FileSchemaNegotiator negotiator) {
    try {
      fsStream = negotiator.fileSystem().openPossiblyCompressedStream(split.getPath());

      // Open streaming reader
      Workbook workbook = StreamingReader.builder()
        .rowCacheSize(ROW_CACHE_SIZE)
        .bufferSize(BUFFER_SIZE)
        .setReadCoreProperties(true)
        .open(fsStream);

      streamingWorkbook = (StreamingWorkbook) workbook;

    } catch (Exception e) {
      throw UserException
        .dataReadError(e)
        .message("Failed to open open input file: %s", split.getPath().toString())
        .addContext(e.getMessage())
        .addContext(errorContext)
        .build(logger);
    }
    sheet = getSheet();

    // Populate Metadata Hashmap
    populateMetadata(streamingWorkbook);
  }

  /**
   * This function defines the schema from the header row.
   */
  private void defineSchema() {
    SchemaBuilder builder = new SchemaBuilder();
    getColumnHeaders(builder);
  }

  private void getColumnHeaders(SchemaBuilder builder) {
    //Get the field names
    int columnCount;

    // Case for empty sheet
    if (sheet.getLastRowNum() == 0) {
      builder.buildSchema();
      return;
    }

    columnWriters = new ArrayList<>();
    metadataColumnWriters = new ArrayList<>();

    rowIterator = sheet.iterator();

    // Get the number of columns.
    // This menthod also advances the row reader to the location of the first row of data
    columnCount = getColumnCount();

    excelFieldNames = new ArrayList<>();
    cellWriterArray = new ArrayList<>();

    //If there are no headers, create columns names of field_n
    if (readerConfig.headerRow == -1) {
      String missingFieldName;
      int i = 0;

      for (Cell c : currentRow) {
        missingFieldName = MISSING_FIELD_NAME_HEADER + (i + 1);
        makeColumn(builder, missingFieldName, TypeProtos.MinorType.VARCHAR);
        excelFieldNames.add(i, missingFieldName);
        i++;
      }
      builder.buildSchema();
    } else if (rowIterator.hasNext()) {
      //Get the header row and column count
      totalColumnCount = currentRow.getLastCellNum();
      Cell dataCell = null;

      //Read the header row
      Iterator<Cell> headerRowIterator = currentRow.cellIterator();
      int colPosition = 0;
      String tempColumnName;

      // Get the first data row.
      currentRow = rowIterator.next();
      Row firstDataRow = currentRow;
      Iterator<Cell> dataRowIterator = firstDataRow.cellIterator();


      while (headerRowIterator.hasNext()) {
        // We need this to get the header names
        Cell cell = headerRowIterator.next();

        // Since header names are most likely all Strings, we need the first row of actual data to get the data types
        try {
          dataCell = dataRowIterator.next();
        } catch (NoSuchElementException e) {
          // Do nothing... empty value in data cell
        }

        switch (dataCell.getCellType()) {
          case STRING:
            tempColumnName = cell.getStringCellValue()
              .replace(PARSER_WILDCARD, SAFE_WILDCARD)
              .replaceAll("\\.", SAFE_SEPARATOR)
              .replaceAll("\\n", HEADER_NEW_LINE_REPLACEMENT);
            makeColumn(builder, tempColumnName, TypeProtos.MinorType.VARCHAR);
            excelFieldNames.add(colPosition, tempColumnName);
            break;
          case FORMULA:
            case NUMERIC:
            tempColumnName = cell.getStringCellValue();
            makeColumn(builder, tempColumnName, TypeProtos.MinorType.FLOAT8);
            excelFieldNames.add(colPosition, tempColumnName);
            break;
        }
        colPosition++;
      }
    }
    addMetadataToSchema(builder);
    builder.buildSchema();
  }

  /**
   * Helper function to get the selected sheet from the configuration
   * @return Sheet The selected sheet
   */
  private Sheet getSheet() {
    int sheetIndex = 0;
    if (!readerConfig.sheetName.isEmpty()) {
      sheetIndex = streamingWorkbook.getSheetIndex(readerConfig.sheetName);
    }

    //If the sheet name is not valid, throw user exception
    if (sheetIndex == -1) {
      throw UserException
        .validationError()
        .message("Could not open sheet " + readerConfig.sheetName)
        .addContext(errorContext)
        .build(logger);
    } else {
      return streamingWorkbook.getSheetAt(sheetIndex);
    }
  }

  /**
   * Returns the column count.  There are a few gotchas here in that we have to know the header row and count the physical number of cells
   * in that row.  This function also has to move the rowIterator object to the first row of data.
   * @return The number of actual columns
   */
  private int getColumnCount() {
    // Initialize
    currentRow = rowIterator.next();
    int rowNumber = readerConfig.headerRow > 0 ? sheet.getFirstRowNum() : 0;

    // If the headerRow is greater than zero, advance the iterator to the first row of data
    // This is unfortunately necessary since the streaming reader eliminated the getRow() method.
    for (int i = 1; i < rowNumber; i++) {
      currentRow = rowIterator.next();
    }

    return currentRow.getPhysicalNumberOfCells();
  }

  @Override
  public boolean next() {
    recordCount = 0;
    while (!rowWriter.isFull()) {
      if (!nextLine(rowWriter)) {
        return false;
      }
    }
    return true;
  }

  private boolean nextLine(RowSetLoader rowWriter) {
    if (sheet.getLastRowNum() == 0) {
      // Case for empty sheet
      return false;
    } else if (recordCount >= readerConfig.lastRow) {
      return false;
    }

    // If the user specified that there are no headers, get the column count
    if (readerConfig.headerRow == -1 && recordCount == 0) {
      totalColumnCount = currentRow.getLastCellNum();
    }

    int colPosition = 0;
    if (readerConfig.firstColumn != 0) {
      colPosition = readerConfig.firstColumn - 1;
    }

    int finalColumn = totalColumnCount;
    if (readerConfig.lastColumn != 0) {
      finalColumn = readerConfig.lastColumn - 1;
    }
    rowWriter.start();
    for (int colWriterIndex = 0; colPosition < finalColumn; colWriterIndex++) {
      Cell cell = currentRow.getCell(colPosition);

      populateColumnArray(cell, colPosition);
      cellWriterArray.get(colWriterIndex).load(cell);

      colPosition++;
    }

    if (firstLine) {
      // Add metadata to column array
      addMetadataWriters();
      firstLine = false;
    }

    // Write the metadata
    writeMetadata();

    rowWriter.save();
    recordCount++;

    if (!rowIterator.hasNext()) {
      return false;
    } else {
      currentRow = rowIterator.next();
      return true;
    }
  }

  private void populateMetadata(StreamingWorkbook streamingWorkbook) {

    CoreProperties fileMetadata = streamingWorkbook.getCoreProperties();

    stringMetadata = new HashMap<>();
    dateMetadata = new HashMap<>();

    // Populate String metadata columns
    stringMetadata.put(IMPLICIT_STRING_COLUMN.CATEGORY.getFieldName(), fileMetadata.getCategory());
    stringMetadata.put(IMPLICIT_STRING_COLUMN.CONTENT_STATUS.getFieldName(), fileMetadata.getContentStatus());
    stringMetadata.put(IMPLICIT_STRING_COLUMN.CONTENT_TYPE.getFieldName(), fileMetadata.getContentType());
    stringMetadata.put(IMPLICIT_STRING_COLUMN.CREATOR.getFieldName(), fileMetadata.getCreator());
    stringMetadata.put(IMPLICIT_STRING_COLUMN.DESCRIPTION.getFieldName(), fileMetadata.getDescription());
    stringMetadata.put(IMPLICIT_STRING_COLUMN.IDENTIFIER.getFieldName(), fileMetadata.getIdentifier());
    stringMetadata.put(IMPLICIT_STRING_COLUMN.KEYWORDS.getFieldName(), fileMetadata.getKeywords());
    stringMetadata.put(IMPLICIT_STRING_COLUMN.LAST_MODIFIED_BY_USER.getFieldName(), fileMetadata.getLastModifiedByUser());
    stringMetadata.put(IMPLICIT_STRING_COLUMN.REVISION.getFieldName(), fileMetadata.getRevision());
    stringMetadata.put(IMPLICIT_STRING_COLUMN.SUBJECT.getFieldName(), fileMetadata.getSubject());
    stringMetadata.put(IMPLICIT_STRING_COLUMN.TITLE.getFieldName(), fileMetadata.getTitle());

    // Populate Timestamp columns
    dateMetadata.put(IMPLICIT_TIMESTAMP_COLUMN.CREATED.getFieldName(), fileMetadata.getCreated());
    dateMetadata.put(IMPLICIT_TIMESTAMP_COLUMN.LAST_PRINTED.getFieldName(), fileMetadata.getLastPrinted());
    dateMetadata.put(IMPLICIT_TIMESTAMP_COLUMN.MODIFIED.getFieldName(), fileMetadata.getModified());
  }

  /**
   * Function to populate the column array
   * @param cell The input cell object
   * @param colPosition The index of the column
   */
  private void populateColumnArray(Cell cell, int colPosition) {
    if (!firstLine) {
      return;
    }

    // Case for empty data cell in first row.  In this case, fall back to string.
    if (cell == null) {
      addColumnToArray(rowWriter, excelFieldNames.get(colPosition), MinorType.VARCHAR, false);
      return;
    }

    CellType cellType = cell.getCellType();
    if (cellType == CellType.STRING || readerConfig.allTextMode) {
      addColumnToArray(rowWriter, excelFieldNames.get(colPosition), MinorType.VARCHAR, false);
    } else if (cellType == CellType.NUMERIC && DateUtil.isCellDateFormatted(cell)) {
      // Case if the column is a date or time
      addColumnToArray(rowWriter, excelFieldNames.get(colPosition), MinorType.TIMESTAMP, false);
    } else if (cellType == CellType.NUMERIC || cellType == CellType.FORMULA) {
      // Case if the column is numeric
      addColumnToArray(rowWriter, excelFieldNames.get(colPosition), MinorType.FLOAT8, false);
    } else {
      logger.warn("Unknown data type. Drill only supports reading NUMERIC and STRING.");
    }
  }

  private void addMetadataToSchema(SchemaBuilder builder) {
    // Add String Metadata columns
    for (IMPLICIT_STRING_COLUMN name : IMPLICIT_STRING_COLUMN.values()) {
      makeColumn(builder, name.getFieldName(), MinorType.VARCHAR);
    }

    // Add Date Column Names
    for (IMPLICIT_TIMESTAMP_COLUMN name : IMPLICIT_TIMESTAMP_COLUMN.values()) {
      makeColumn(builder, name.getFieldName(), MinorType.TIMESTAMP);
    }
  }

  private void makeColumn(SchemaBuilder builder, String name, TypeProtos.MinorType type) {
    // Verify supported types
    switch (type) {
      // The Excel Reader only Supports Strings, Floats and Date/Times
      case VARCHAR:
      case FLOAT8:
      case DATE:
      case TIMESTAMP:
      case TIME:
        builder.addNullable(name, type);
        break;
      default:
        throw UserException
          .validationError()
          .message("Undefined column types")
          .addContext("Field name", name)
          .addContext("Type", type.toString())
          .addContext(errorContext)
          .build(logger);
    }
  }

  private void addColumnToArray(TupleWriter rowWriter, String name, TypeProtos.MinorType type, boolean isMetadata) {
    int index = rowWriter.tupleSchema().index(name);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(name, type, TypeProtos.DataMode.OPTIONAL);
      if (isMetadata) {
        colSchema.setBooleanProperty(ColumnMetadata.EXCLUDE_FROM_WILDCARD, true);
      }
      index = rowWriter.addColumn(colSchema);
    } else {
      return;
    }

    if (isMetadata) {
      metadataColumnWriters.add(rowWriter.scalar(index));
    } else {
      columnWriters.add(rowWriter.scalar(index));
      if (readerConfig.allTextMode && type == MinorType.FLOAT8) {
        cellWriterArray.add(new NumericStringWriter(columnWriters.get(index)));
      } else if (type == MinorType.VARCHAR) {
        cellWriterArray.add(new StringCellWriter(columnWriters.get(index)));
      } else if (type == MinorType.FLOAT8) {
        cellWriterArray.add(new NumericCellWriter(columnWriters.get(index)));
      } else if (type == MinorType.TIMESTAMP) {
        cellWriterArray.add(new TimestampCellWriter(columnWriters.get(index)));
      }
    }
  }

  private void addMetadataWriters() {
    for (IMPLICIT_STRING_COLUMN colName : IMPLICIT_STRING_COLUMN.values()) {
      addColumnToArray(rowWriter, colName.getFieldName(), MinorType.VARCHAR, true);
    }
    for (IMPLICIT_TIMESTAMP_COLUMN colName : IMPLICIT_TIMESTAMP_COLUMN.values()) {
      addColumnToArray(rowWriter, colName.getFieldName(), MinorType.TIMESTAMP, true);
    }
  }

  private void writeMetadata() {
    for (IMPLICIT_STRING_COLUMN column : IMPLICIT_STRING_COLUMN.values()) {
      String value = stringMetadata.get(column.getFieldName());
      int index = column.ordinal();
      if (value == null) {
        metadataColumnWriters.get(index).setNull();
      } else {
        metadataColumnWriters.get(index).setString(value);
      }
    }

    for (IMPLICIT_TIMESTAMP_COLUMN column : IMPLICIT_TIMESTAMP_COLUMN.values()) {
      Date timeValue = dateMetadata.get(column.getFieldName());
      int index = column.ordinal() + IMPLICIT_STRING_COLUMN.values().length;
      if (timeValue == null) {
        metadataColumnWriters.get(index).setNull();
      } else {
        metadataColumnWriters.get(index).setTimestamp(new Instant(timeValue));
      }
    }
  }

  @Override
  public void close() {
    if (streamingWorkbook != null) {
      try {
        streamingWorkbook.close();
      } catch (IOException e) {
        logger.warn("Error when closing Excel Workbook resource: {}", e.getMessage());
      }
      streamingWorkbook = null;
    }

    if (fsStream != null) {
      try {
        fsStream.close();
      } catch (IOException e) {
        logger.warn("Error when closing Excel File Stream resource: {}", e.getMessage());
      }
      fsStream = null;
    }
  }

  public static class CellWriter {
    ScalarWriter columnWriter;

    CellWriter(ScalarWriter columnWriter) {
      this.columnWriter = columnWriter;
    }

    public void load(Cell cell) {}
  }

  public class StringCellWriter extends ExcelBatchReader.CellWriter {
    StringCellWriter(ScalarWriter columnWriter) {
      super(columnWriter);
    }

    public void load(Cell cell) {
      if (cell == null) {
        columnWriter.setNull();
      } else {
        String fieldValue = cell.getStringCellValue();
        if (fieldValue == null && readerConfig.allTextMode) {
          fieldValue = String.valueOf(cell.getNumericCellValue());
        }
        columnWriter.setString(fieldValue);
      }
    }
  }

  public static class NumericStringWriter extends ExcelBatchReader.CellWriter {
    NumericStringWriter(ScalarWriter columnWriter) {
      super(columnWriter);
    }

    public void load(Cell cell) {
      if (cell == null) {
        columnWriter.setNull();
      } else {
        String fieldValue = String.valueOf(cell.getNumericCellValue());
        columnWriter.setString(fieldValue);
      }
    }
  }

  public static class NumericCellWriter extends ExcelBatchReader.CellWriter {
    NumericCellWriter(ScalarWriter columnWriter) {
      super(columnWriter);
    }

    public void load(Cell cell) {
      if (cell == null) {
        columnWriter.setNull();
      } else {
        double fieldNumValue = cell.getNumericCellValue();
        columnWriter.setDouble(fieldNumValue);
      }
    }
  }

  public static class TimestampCellWriter extends ExcelBatchReader.CellWriter {
    TimestampCellWriter(ScalarWriter columnWriter) {
      super(columnWriter);
    }

    public void load(Cell cell) {
      if (cell == null) {
        columnWriter.setNull();
      } else {
        logger.debug("Cell value: {}", cell.getNumericCellValue());
        Date dt = DateUtil.getJavaDate(cell.getNumericCellValue(), TimeZone.getTimeZone("UTC"));
        Instant timeStamp = new Instant(dt.toInstant().getEpochSecond() * 1000);
        columnWriter.setTimestamp(timeStamp);
      }
    }
  }
}
