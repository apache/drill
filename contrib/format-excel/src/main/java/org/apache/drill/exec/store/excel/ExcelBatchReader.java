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
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.CellValue;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.joda.time.Instant;
import org.slf4j.Logger;

import java.io.InputStream;
import java.util.Date;
import java.util.Iterator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.TimeZone;

public class ExcelBatchReader implements ManagedReader<FileSchemaNegotiator> {

  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(ExcelBatchReader.class);

  private static final String SAFE_WILDCARD = "_$";

  private static final String SAFE_SEPARATOR = "_";

  private static final String PARSER_WILDCARD = ".*";

  private static final String HEADER_NEW_LINE_REPLACEMENT = "__";

  private static final String MISSING_FIELD_NAME_HEADER = "field_";

  private final ExcelReaderConfig readerConfig;

  private XSSFSheet sheet;

  private XSSFWorkbook workbook;

  private InputStream fsStream;

  private FormulaEvaluator evaluator;

  private ArrayList<String> excelFieldNames;

  private ArrayList<ScalarWriter> columnWriters;

  private ArrayList<CellType> cellTypes;

  private ArrayList<CellWriter> cellWriterArray;

  private Iterator<Row> rowIterator;

  private RowSetLoader rowWriter;

  private int totalColumnCount;

  private int lineCount;

  private boolean firstLine;

  private FileSplit split;

  private ResultSetLoader loader;

  private int recordCount;

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
    loader = negotiator.build();
    rowWriter = loader.writer();
    openFile(negotiator);
    defineSchema();
    return true;
  }

  private void openFile(FileScanFramework.FileSchemaNegotiator negotiator) {
    try {
      fsStream = negotiator.fileSystem().openPossiblyCompressedStream(split.getPath());
      workbook = new XSSFWorkbook(fsStream);
    } catch (Exception e) {
      throw UserException
        .dataReadError(e)
        .message("Failed to open open input file: %s", split.getPath().toString())
        .message(e.getMessage())
        .build(logger);
    }

    // Evaluate formulae
    evaluator = workbook.getCreationHelper().createFormulaEvaluator();

    workbook.setMissingCellPolicy(Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
    sheet = getSheet();
  }

  /**
   * This function defines the schema from the header row.
   * @return TupleMedata of the discovered schema
   */
  private TupleMetadata defineSchema() {
    SchemaBuilder builder = new SchemaBuilder();
    return getColumnHeaders(builder);
  }

  private TupleMetadata getColumnHeaders(SchemaBuilder builder) {
    //Get the field names
    int columnCount = 0;

    // Case for empty sheet.
    if (sheet.getFirstRowNum() == 0 && sheet.getLastRowNum() == 0) {
      return builder.buildSchema();
    }

    // Get the number of columns.
    columnCount = getColumnCount();

    excelFieldNames = new ArrayList<>(columnCount);
    cellWriterArray = new ArrayList<>(columnCount);
    rowIterator = sheet.iterator();

    //If there are no headers, create columns names of field_n
    if (readerConfig.headerRow == -1) {
      String missingFieldName;
      for (int i = 0; i < columnCount; i++) {
        missingFieldName = MISSING_FIELD_NAME_HEADER + (i + 1);
        makeColumn(builder, missingFieldName, TypeProtos.MinorType.VARCHAR);
        excelFieldNames.add(i, missingFieldName);
      }
      columnWriters = new ArrayList<>(excelFieldNames.size());
      cellTypes = new ArrayList<>(excelFieldNames.size());

      return builder.buildSchema();
    } else if (rowIterator.hasNext()) {
      //Find the header row
      int firstHeaderRow = getFirstHeaderRow();

      while (lineCount < firstHeaderRow) {
        Row row = rowIterator.next();
        lineCount++;
      }
      //Get the header row and column count
      Row row = rowIterator.next();
      totalColumnCount = row.getLastCellNum();
      cellTypes = new ArrayList<>(totalColumnCount);

      //Read the header row
      Iterator<Cell> cellIterator = row.cellIterator();
      int colPosition = 0;
      String tempColumnName = "";

      while (cellIterator.hasNext()) {
        Cell cell = cellIterator.next();

        CellValue cellValue = evaluator.evaluate(cell);
        switch (cellValue.getCellType()) {
          case STRING:
            tempColumnName = cell.getStringCellValue()
              .replace(PARSER_WILDCARD, SAFE_WILDCARD)
              .replaceAll("\\.", SAFE_SEPARATOR)
              .replaceAll("\\n", HEADER_NEW_LINE_REPLACEMENT);
            makeColumn(builder, tempColumnName, TypeProtos.MinorType.VARCHAR);
            excelFieldNames.add(colPosition, tempColumnName);
            cellTypes.add(CellType.STRING);
            break;
          case NUMERIC:
            tempColumnName = String.valueOf(cell.getNumericCellValue());
            makeColumn(builder, tempColumnName, TypeProtos.MinorType.FLOAT8);
            excelFieldNames.add(colPosition, tempColumnName);
            cellTypes.add(CellType.NUMERIC);
            break;
        }
        colPosition++;
      }
    }
    columnWriters = new ArrayList<>(excelFieldNames.size());
    return builder.buildSchema();
  }

  /**
   * Helper function to get the selected sheet from the configuration
   *
   * @return XSSFSheet The selected sheet
   */
  private XSSFSheet getSheet() {
    int sheetIndex = 0;
    if (!readerConfig.sheetName.isEmpty()) {
      sheetIndex = workbook.getSheetIndex(readerConfig.sheetName);
    }

    //If the sheet name is not valid, throw user exception
    if (sheetIndex == -1) {
      throw UserException
        .validationError()
        .message("Could not open sheet " + readerConfig.sheetName)
        .build(logger);
    } else {
      return workbook.getSheetAt(sheetIndex);
    }
  }

  /**
   * Returns the column count.  There are a few gotchas here in that we have to know the header row and count the physical number of cells
   * in that row.  Since the user can define the header row,
   * @return The number of actual columns
   */
  private int getColumnCount() {
    int rowNumber = readerConfig.headerRow > 0 ? sheet.getFirstRowNum() : 0;
    XSSFRow sheetRow = sheet.getRow(rowNumber);

    return sheetRow != null ? sheetRow.getPhysicalNumberOfCells() : 0;
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
    if( sheet.getFirstRowNum() == 0 && sheet.getLastRowNum() == 0) {
      // Case for empty sheet
      return false;
    } else if (!rowIterator.hasNext()) {
      return false;
    } else if (recordCount >= readerConfig.lastRow) {
      return false;
    }

    int lastRow = readerConfig.lastRow;
    if (recordCount < lastRow && rowIterator.hasNext()) {
      lineCount++;

      Row row = rowIterator.next();

      // If the user specified that there are no headers, get the column count
      if (readerConfig.headerRow == -1 && recordCount == 0) {
        this.totalColumnCount = row.getLastCellNum();
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
      for (int colWriterIndex = 0; colPosition < finalColumn; colPosition++) {
        Cell cell = row.getCell(colPosition);

        CellValue cellValue = evaluator.evaluate(cell);

        populateColumnArray(cell, cellValue, colPosition);
        cellWriterArray.get(colWriterIndex).load(cell);

        colWriterIndex++;
      }

      if (firstLine) {
        firstLine = false;
      }
      rowWriter.save();
      recordCount++;
      return true;
    } else {
      return false;
    }

  }

  /**
   * Function to populate the column array
   * @param cell The input cell object
   * @param cellValue The cell value
   * @param colPosition The index of the column
   */
  private void populateColumnArray(Cell cell, CellValue cellValue, int colPosition) {
    if (!firstLine) {
      return;
    }

    if (cellValue == null) {
      addColumnToArray(rowWriter, excelFieldNames.get(colPosition), MinorType.VARCHAR);
    } else {
      CellType cellType = cellValue.getCellType();
      if (cellType == CellType.STRING || readerConfig.allTextMode) {
        addColumnToArray(rowWriter, excelFieldNames.get(colPosition), MinorType.VARCHAR);
      } else if (cellType == CellType.NUMERIC && DateUtil.isCellDateFormatted(cell)) {
        addColumnToArray(rowWriter, excelFieldNames.get(colPosition), MinorType.TIMESTAMP);
      } else if (cellType == CellType.NUMERIC) {
        addColumnToArray(rowWriter, excelFieldNames.get(colPosition), MinorType.FLOAT8);
      } else {
        logger.warn("Unknown data type. Drill only supports reading NUMERIC and STRING.");
      }
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
          .build(logger);
    }
  }

  private void addColumnToArray(TupleWriter rowWriter, String name, TypeProtos.MinorType type) {
    int index = rowWriter.tupleSchema().index(name);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(name, type, TypeProtos.DataMode.OPTIONAL);
      index = rowWriter.addColumn(colSchema);
    } else {
      return;
    }

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

  /**
   * Returns the index of the first row of actual data.  This function is to be used to find the header row as the POI skips blank rows.
   * @return The headerRow index, corrected for blank rows
   */
  private int getFirstHeaderRow() {
    int firstRow = sheet.getFirstRowNum();
    int headerRow = readerConfig.headerRow;

    if (headerRow < 0) {
      return firstRow;
    } else {
      return headerRow;
    }
  }

  @Override
  public void close() {
    if (workbook != null) {
      try {
        workbook.close();
      } catch (IOException e) {
        logger.warn("Error when closing XSSFWorkbook resource: {}", e.getMessage());
      }
      workbook = null;
    }

    if (fsStream != null) {
      try {
        fsStream.close();
      } catch (IOException e) {
        logger.warn("Error when closing XSSFWorkbook resource: {}", e.getMessage());
      }
      fsStream = null;
    }
  }

  public class CellWriter {
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
      CellValue cellValue = evaluator.evaluate(cell);
      if (cellValue == null) {
        columnWriter.setNull();
      } else {
        String fieldValue = cellValue.getStringValue();
        if (fieldValue == null && readerConfig.allTextMode) {
          fieldValue = String.valueOf(cell.getNumericCellValue());
        }
        columnWriter.setString(fieldValue);
      }
    }
  }

  public class NumericStringWriter extends ExcelBatchReader.CellWriter {
    NumericStringWriter(ScalarWriter columnWriter) {
      super(columnWriter);
    }

    public void load(Cell cell) {
      String fieldValue = String.valueOf(cell.getNumericCellValue());

      if (fieldValue == null) {
        columnWriter.setNull();
      } else {
        columnWriter.setString(fieldValue);
      }
    }
  }

  public class NumericCellWriter extends ExcelBatchReader.CellWriter {
    NumericCellWriter(ScalarWriter columnWriter) {
      super(columnWriter);
    }

    public void load(Cell cell) {
      CellValue cellValue = evaluator.evaluate(cell);
      if (cellValue == null) {
        columnWriter.setNull();
      } else {
        double fieldNumValue = cellValue.getNumberValue();
        columnWriter.setDouble(fieldNumValue);
      }
    }
  }

  public class TimestampCellWriter extends ExcelBatchReader.CellWriter {
    TimestampCellWriter(ScalarWriter columnWriter) {
      super(columnWriter);
    }

    public void load(Cell cell) {
      CellValue cellValue = evaluator.evaluate(cell);

      if (cellValue == null) {
        columnWriter.setNull();
      } else {
        logger.debug("Cell value: {}", cellValue.getNumberValue());

        Date dt = DateUtil.getJavaDate(cellValue.getNumberValue(), TimeZone.getTimeZone("UTC"));
        Instant timeStamp = new Instant(dt.toInstant().getEpochSecond() * 1000);
        columnWriter.setTimestamp(timeStamp);
      }
    }
  }
}
