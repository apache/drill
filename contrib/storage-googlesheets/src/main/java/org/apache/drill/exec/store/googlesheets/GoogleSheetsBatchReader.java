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

package org.apache.drill.exec.store.googlesheets;

import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.Sheet;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.googlesheets.columns.GoogleSheetsColumnWriter.GoogleSheetsBigIntegerColumnWriter;
import org.apache.drill.exec.store.googlesheets.columns.GoogleSheetsColumnWriter.GoogleSheetsBooleanColumnWriter;
import org.apache.drill.exec.store.googlesheets.columns.GoogleSheetsColumnWriter.GoogleSheetsDateColumnWriter;
import org.apache.drill.exec.store.googlesheets.columns.GoogleSheetsColumnWriter.GoogleSheetsFloatColumnWriter;
import org.apache.drill.exec.store.googlesheets.columns.GoogleSheetsColumnWriter.GoogleSheetsIntegerColumnWriter;
import org.apache.drill.exec.store.googlesheets.columns.GoogleSheetsColumnWriter.GoogleSheetsNumericColumnWriter;
import org.apache.drill.exec.store.googlesheets.columns.GoogleSheetsColumnWriter.GoogleSheetsTimeColumnWriter;
import org.apache.drill.exec.store.googlesheets.columns.GoogleSheetsColumnWriter.GoogleSheetsTimestampColumnWriter;
import org.apache.drill.exec.store.googlesheets.columns.GoogleSheetsColumnWriter.GoogleSheetsVarcharColumnWriter;
import org.apache.drill.exec.store.googlesheets.utils.GoogleSheetsRangeBuilder;
import org.apache.drill.exec.store.googlesheets.utils.GoogleSheetsUtils;
import org.apache.drill.exec.util.Utilities;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GoogleSheetsBatchReader implements ManagedReader<SchemaNegotiator> {
  private static final Logger logger = LoggerFactory.getLogger(GoogleSheetsBatchReader.class);

  // The default batch size is 1k rows.  It appears that Google sets the maximum batch size at 1000
  // rows. There is conflicting information about this online, but during testing, ranges with more than
  // 1000 rows would throw invalid request errors.
  protected static final int BATCH_SIZE = 1000;
  private static final String SHEET_COLUMN_NAME = "_sheets";
  private static final String TITLE_COLUMN_NAME = "_title";

  private static final List<String> IMPLICIT_FIELDS =  Arrays.asList(SHEET_COLUMN_NAME, TITLE_COLUMN_NAME);

  private final GoogleSheetsStoragePluginConfig config;
  private final GoogleSheetsSubScan subScan;
  private final List<SchemaPath> projectedColumns;
  private final Sheet sheet;
  private final Sheets service;
  private final GoogleSheetsRangeBuilder rangeBuilder;
  private final String sheetID;
  private final List<String> sheetNames;
  private CustomErrorContext errorContext;
  private ScalarWriter sheetNameWriter;
  private ScalarWriter titleNameWriter;
  private TupleMetadata schema;
  private Map<String, GoogleSheetsColumn> columnMap;
  private RowSetLoader rowWriter;

  public GoogleSheetsBatchReader(GoogleSheetsStoragePluginConfig config, GoogleSheetsSubScan subScan, GoogleSheetsStoragePlugin plugin) {
    this.config = config;
    this.subScan = subScan;
    this.projectedColumns = subScan.getColumns();
    this.service = plugin.getSheetsService(subScan.getUserName());
    this.sheetID = subScan.getScanSpec().getSheetID();
    this.sheetNames = new ArrayList<>();
    try {
      List<Sheet> tabList = GoogleSheetsUtils.getTabList(service, sheetID);
      this.sheet = tabList.get(subScan.getScanSpec().getTabIndex());
    } catch (IOException e) {
      throw UserException.connectionError(e)
        .message("Could not find tab with index " + subScan.getScanSpec().getTabIndex())
        .build(logger);
    }

    int maxRecords = subScan.getMaxRecords();
    this.rangeBuilder = new GoogleSheetsRangeBuilder(subScan.getScanSpec().getTableName(), BATCH_SIZE)
      .addRowCount(sheet.getProperties().getGridProperties().getRowCount());
    if (maxRecords > 0) {
      // Since the headers are in the first row, add one more row of records to the batch.
      if (config.getExtractHeaders()) {
        rangeBuilder.addLimit(maxRecords + 1);
      } else {
        rangeBuilder.addLimit(maxRecords);
      }
    }
  }

  @Override
  public boolean open(SchemaNegotiator negotiator) {
    logger.debug("Opening Google Sheet {}", subScan.getScanSpec().getTableName());
    this.errorContext = negotiator.parentErrorContext();

    // Build Schema
    String tableName = subScan.getScanSpec().getTableName();
    String pluginName = subScan.getScanSpec().getSheetID();

    try {
      columnMap = GoogleSheetsUtils.getColumnMap(GoogleSheetsUtils.getFirstRows(service, pluginName, tableName), projectedColumns, config.allTextMode());

      // Get sheet list for metadata.
      List<Sheet> sheetList = GoogleSheetsUtils.getTabList(service, pluginName);
      for (Sheet sheet : sheetList) {
        sheetNames.add(sheet.getProperties().getTitle());
      }
    } catch (IOException e) {
      throw UserException.validationError(e)
        .message("Error building schema: " + e.getMessage())
        .addContext(errorContext)
        .build(logger);
    }

    // For a star query, we can assume that all columns are projected, and thus,
    // we can construct a range with the first and last column letters.  IE: A:F
    // For additional fun, column indexing starts at one rather than zero, so the columnIndex,
    // which starts at zero needs to be incremented by one to match the letter notation.  IE:
    // A = 1, B = 2...
    // We cannot assume that the columns are in order, so it is necessary to iterate over the
    // list of columns.
    if (isStarQuery()) {
      int minIndex = 1;
      int maxIndex = 1;
      for (GoogleSheetsColumn column : columnMap.values()) {
        if ((column.getColumnIndex() + 1) < minIndex) {
          minIndex = column.getColumnIndex();
        } else if ((column.getColumnIndex() + 1) > maxIndex ) {
          maxIndex = column.getColumnIndex() + 1;
        }
      }
      logger.debug("Min index: {}, max index: {}", minIndex, maxIndex);
      rangeBuilder.isStarQuery(true);
      rangeBuilder.addFirstColumn(GoogleSheetsUtils.columnToLetter(minIndex))
        .addLastColumn(GoogleSheetsUtils.columnToLetter(maxIndex));
    } else {
      // For non-star queries, we need to build a range which consists of
      // multiple columns.  For example, let's say that we wanted to project
      // columns 1-3,5,7-9.  We'd need to construct ranges like this:
      // A-C,E,G-I
      rangeBuilder.isStarQuery(false);
      rangeBuilder.addProjectedRanges(GoogleSheetsUtils.getProjectedRanges(tableName, columnMap));
    }

    // Add the max row count from the sheet metadata
    rangeBuilder.addRowCount(sheet.getProperties().getGridProperties().getRowCount());
    logger.debug(rangeBuilder.toString());

    // Add provided schema if present.
    if (negotiator.hasProvidedSchema()) {
      schema = negotiator.providedSchema();
    } else {
      schema = GoogleSheetsUtils.buildSchema(columnMap);
    }

    // Add implicit metadata to schema
    ColumnMetadata sheetImplicitColumn = MetadataUtils.newScalar(SHEET_COLUMN_NAME, MinorType.VARCHAR, DataMode.REPEATED);
    sheetImplicitColumn.setBooleanProperty(ColumnMetadata.EXCLUDE_FROM_WILDCARD, true);
    schema.addColumn(sheetImplicitColumn);

    ColumnMetadata titleImplicitColumn = MetadataUtils.newScalar(TITLE_COLUMN_NAME,
      MinorType.VARCHAR, DataMode.OPTIONAL);
    titleImplicitColumn.setBooleanProperty(ColumnMetadata.EXCLUDE_FROM_WILDCARD, true);
    schema.addColumn(titleImplicitColumn);


    negotiator.tableSchema(schema, true);
    ResultSetLoader resultLoader = negotiator.build();
    // Create ScalarWriters
    rowWriter = resultLoader.writer();

    if (negotiator.hasProvidedSchema()) {
      setColumnWritersFromProvidedSchema(schema);
    } else {
      // Build writers
      MinorType dataType;
      for (GoogleSheetsColumn column : columnMap.values()) {
        // Ignore metadata columns.
        if (column.isMetadata()) {
          continue;
        }
        dataType = column.getDrillDataType();
        if (dataType == MinorType.FLOAT8) {
          column.setWriter(new GoogleSheetsNumericColumnWriter(rowWriter, column.getColumnName()));
        } else if (dataType == MinorType.VARCHAR) {
          column.setWriter(new GoogleSheetsVarcharColumnWriter(rowWriter, column.getColumnName()));
        } else if (dataType == MinorType.DATE) {
          column.setWriter(new GoogleSheetsDateColumnWriter(rowWriter, column.getColumnName()));
        } else if (dataType == MinorType.TIMESTAMP) {
          column.setWriter(new GoogleSheetsTimestampColumnWriter(rowWriter, column.getColumnName()));
        } else if (dataType == MinorType.TIME) {
          column.setWriter(new GoogleSheetsTimeColumnWriter(rowWriter, column.getColumnName()));
        } else if (dataType == MinorType.BIT) {
          column.setWriter(new GoogleSheetsBooleanColumnWriter(rowWriter, column.getColumnName()));
        }
      }
    }
    return true;
  }

  @Override
  public boolean next() {
    logger.debug("Processing batch.");
    return processRow();
  }

  private boolean processRow() {
    List<List<Object>> data;
    try {
      if (isStarQuery()) {
        // Get next range
        String range = rangeBuilder.next();
        if (range == null) {
          rangeBuilder.lastBatch();
          return false;
        }
        data = GoogleSheetsUtils.getDataFromRange(service, sheetID, range);
      } else {
        List<String> batches = rangeBuilder.nextBatch();
        if (batches == null) {
          rangeBuilder.lastBatch();
          return false;
        } else if (!batches.isEmpty()) {
          data = GoogleSheetsUtils.getBatchData(service, sheetID, batches);
        } else {
          data = Collections.emptyList();
        }
      }
    } catch (IOException e) {
      throw UserException.dataReadError(e)
        .message("Error reading Google Sheet: " + e.getMessage())
        .addContext(errorContext)
        .build(logger);
    }

    int colIndex;
    List<Object> row;
    int startIndex = 0;
    Object value;
    if (config.getExtractHeaders()) {
      startIndex = 1;
    }

    // Edge Case:  If only metadata columns are projected, project one row and return
    if (data.size() == 0 && onlyMetadata(schema)) {
      rowWriter.start();
      projectMetadata();
      rowWriter.save();
    }

    for (int rowIndex = startIndex; rowIndex < data.size(); rowIndex++) {
      rowWriter.start();
      row = data.get(rowIndex);
      for (GoogleSheetsColumn column : columnMap.values()) {
        colIndex = column.getDrillColumnIndex();
        try {
          value = row.get(colIndex);
        } catch (IndexOutOfBoundsException e) {
          // This is a bit of an edge case.  In some circumstances, if there is a null value at the end of
          // a row, instead of returning an empty string, Google Sheets will shorten the row. This does not
          // occur if the null value appears in the middle or beginning of a row, or even all the time. This check
          // prevents out of bounds errors and moves on to the next row.
          continue;
        }
        column.load(value);
      }
      projectMetadata();
      rowWriter.save();
    }

    // If there is another batch, return true
    if (rowWriter.rowCount() + BATCH_SIZE < rangeBuilder.getRowCount()) {
      return true;
    } else {
      // If the results contained less than the batch size, stop iterating.
      rangeBuilder.lastBatch();
      return false;
    }
  }

  private void projectMetadata() {
    // Add metadata
    if (sheetNameWriter == null) {
      int sheetColumnIndex = rowWriter.tupleSchema().index(SHEET_COLUMN_NAME);
      if (sheetColumnIndex == -1) {
        ColumnMetadata colSchema = MetadataUtils.newScalar(SHEET_COLUMN_NAME, MinorType.VARCHAR, DataMode.REPEATED);
        colSchema.setBooleanProperty(ColumnMetadata.EXCLUDE_FROM_WILDCARD, true);
      }
      sheetNameWriter = rowWriter.column(SHEET_COLUMN_NAME).array().scalar();
    }

    for (String sheetName : sheetNames) {
      sheetNameWriter.setString(sheetName);
    }

    if (titleNameWriter == null) {
      int titleColumnIndex = rowWriter.tupleSchema().index(TITLE_COLUMN_NAME);
      if (titleColumnIndex == -1) {
        ColumnMetadata titleColSchema = MetadataUtils.newScalar(TITLE_COLUMN_NAME,
          MinorType.VARCHAR,
          DataMode.OPTIONAL);
        titleColSchema.setBooleanProperty(ColumnMetadata.EXCLUDE_FROM_WILDCARD, true);
      }
      titleNameWriter = rowWriter.column(TITLE_COLUMN_NAME).scalar();
    }
    titleNameWriter.setString(subScan.getScanSpec().getFileName());
  }

  /**
   * Returns true if the projected schema only contains implicit metadata columns.
   * @param schema {@link TupleMetadata} The active schema
   * @return True if the schema is only metadata, false otherwise.
   */
  private boolean onlyMetadata(TupleMetadata schema) {
    for (MaterializedField field: schema.toFieldList()) {
      if (!IMPLICIT_FIELDS.contains(field.getName())){
        return false;
      }
    }
    return true;
  }

  private void setColumnWritersFromProvidedSchema(TupleMetadata schema) {
    List<MaterializedField> fieldList = schema.toFieldList();

    MinorType dataType;
    GoogleSheetsColumn column;
    for (MaterializedField field: fieldList) {
      dataType = field.getType().getMinorType();
      column = columnMap.get(field.getName());

      // Do not create a column writer object for metadata columns
      if (column == null || column.isMetadata()) {
        continue;
      }

      // Get the field
      if (dataType == MinorType.FLOAT8) {
        column.setWriter(new GoogleSheetsNumericColumnWriter(rowWriter, column.getColumnName()));
      } else if (dataType == MinorType.FLOAT4) {
        column.setWriter(new GoogleSheetsFloatColumnWriter(rowWriter, column.getColumnName()));
      } else if (dataType == MinorType.VARCHAR) {
        column.setWriter(new GoogleSheetsVarcharColumnWriter(rowWriter, column.getColumnName()));
      } else if (dataType == MinorType.INT) {
        column.setWriter(new GoogleSheetsIntegerColumnWriter(rowWriter, column.getColumnName()));
      } else if (dataType == MinorType.BIGINT) {
        column.setWriter(new GoogleSheetsBigIntegerColumnWriter(rowWriter, column.getColumnName()));
      } else if (dataType == MinorType.DATE) {
        column.setWriter(new GoogleSheetsDateColumnWriter(rowWriter, column.getColumnName()));
      } else if (dataType == MinorType.TIMESTAMP) {
        column.setWriter(new GoogleSheetsTimestampColumnWriter(rowWriter, column.getColumnName()));
      } else if (dataType == MinorType.TIME) {
        column.setWriter(new GoogleSheetsTimeColumnWriter(rowWriter, column.getColumnName()));
      } else if (dataType == MinorType.BIT) {
        column.setWriter(new GoogleSheetsBooleanColumnWriter(rowWriter, column.getColumnName()));
      } else {
        throw UserException.validationError()
          .message(dataType + " is not supported for GoogleSheets.")
          .build(logger);
      }
    }
  }

  /**
   * This function is necessary for star or aggregate queries.
   * @return True if the query is a star or aggregate query, false if not.
   */
  private boolean isStarQuery() {
    return (Utilities.isStarQuery(projectedColumns)) || projectedColumns.size() == 0;
  }

  @Override
  public void close() {
    // Do nothing
  }
}
