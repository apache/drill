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
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.googlesheets.columns.GoogleSheetsDateColumnWriter;
import org.apache.drill.exec.store.googlesheets.columns.GoogleSheetsNumericColumnWriter;
import org.apache.drill.exec.store.googlesheets.columns.GoogleSheetsTimeColumnWriter;
import org.apache.drill.exec.store.googlesheets.columns.GoogleSheetsTimestampColumnWriter;
import org.apache.drill.exec.store.googlesheets.columns.GoogleSheetsVarcharColumnWriter;
import org.apache.drill.exec.store.googlesheets.utils.GoogleSheetsRangeBuilder;
import org.apache.drill.exec.store.googlesheets.utils.GoogleSheetsUtils;
import org.apache.drill.exec.util.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class GoogleSheetsBatchReader implements ManagedReader<SchemaNegotiator> {
  private static final Logger logger = LoggerFactory.getLogger(GoogleSheetsBatchReader.class);

  // The default batch size is 1k rows.  It appears that Google sets the maximum batch size at 1000
  // rows. There is conflicting information about this online, but during testing, ranges with more than
  // 1000 rows would throw invalid request errors.
  private static final int BATCH_SIZE = 1000;

  private final GoogleSheetsStoragePluginConfig config;
  private final GoogleSheetsSubScan subScan;
  private final List<SchemaPath> projectedColumns;
  private final Sheet sheet;
  private final Sheets service;
  private final GoogleSheetsRangeBuilder rangeBuilder;
  private final String sheetID;
  private CustomErrorContext errorContext;
  private Map<String, GoogleSheetsColumn> columnMap;
  private RowSetLoader rowWriter;

  public GoogleSheetsBatchReader(GoogleSheetsStoragePluginConfig config, GoogleSheetsSubScan subScan, GoogleSheetsStoragePlugin plugin) {
    this.config = config;
    this.subScan = subScan;
    this.projectedColumns = subScan.getColumns();
    this.service = plugin.getSheetsService(subScan.getUserName());
    this.sheetID = subScan.getScanSpec().getSheetID();
    try {
      List<Sheet> sheetList = GoogleSheetsUtils.getSheetList(service, sheetID);
      this.sheet = sheetList.get(subScan.getScanSpec().getTabIndex());
    } catch (IOException e) {
      throw UserException.connectionError(e)
        .message("Could not find tab with index " + subScan.getScanSpec().getTabIndex())
        .build(logger);
    }

    int maxRecords = subScan.getMaxRecords();
    this.rangeBuilder = new GoogleSheetsRangeBuilder(subScan.getScanSpec().getTableName(), BATCH_SIZE)
      .addRowCount(sheet.getProperties().getGridProperties().getRowCount());
    if (maxRecords > 0) {
      rangeBuilder.addLimit(maxRecords);
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
    TupleMetadata schema;
    if (negotiator.hasProvidedSchema()) {
      schema = negotiator.providedSchema();
    } else {
      schema = GoogleSheetsUtils.buildSchema(columnMap);
    }
    negotiator.tableSchema(schema, true);
    ResultSetLoader resultLoader = negotiator.build();
    // Create ScalarWriters
    rowWriter = resultLoader.writer();

    // Build writers
    MinorType dataType;
    for (GoogleSheetsColumn column : columnMap.values()) {
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
      }
    }
    return true;
  }

  @Override
  public boolean next() {
    logger.debug("Processing batch.");
    while (!rowWriter.isFull()) {
      if (!processRow()) {
        return false;
      }
    }
    return true;
  }

  private boolean processRow() {
    List<List<Object>> data;
    try {
      if (isStarQuery()) {
        // Get next range
        String range = rangeBuilder.next();
        if (range == null) {
          return false;
        }
        data = GoogleSheetsUtils.getDataFromRange(service, sheetID, range);
      } else {
        List<String> batches = rangeBuilder.nextBatch();
        data = GoogleSheetsUtils.getBatchData(service, sheetID, batches);
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
      rowWriter.save();
    }

    // If the results contained less than the batch size, stop iterating.
    if (rowWriter.rowCount() < BATCH_SIZE) {
      rangeBuilder.lastBatch();
      return false;
    }
    return true;
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
