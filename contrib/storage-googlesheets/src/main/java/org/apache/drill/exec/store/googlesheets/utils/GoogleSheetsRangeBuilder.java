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

package org.apache.drill.exec.store.googlesheets.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.exec.store.googlesheets.columns.GoogleSheetsColumnRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This class is used to construct a range with the GoogleSheet reader in Drill.  This
 * builder generates ranges and can apply the projection and limit pushdowns to the ranges.
 *
 * GoogleSheets uses A1 notation for defining columns and ranges. An example would be:
 * 'Sheet1'!A11:F20
 *
 */
public class GoogleSheetsRangeBuilder implements Iterator<String> {
  private static final Logger logger = LoggerFactory.getLogger(GoogleSheetsRangeBuilder.class);

  private final List<String> columns;
  private final String sheetName;
  private final int batchSize;
  private List<GoogleSheetsColumnRange> projectedRanges;
  private int limit;
  private boolean hasMore;
  private int batchIndex;
  private boolean isStarQuery;
  private String firstColumn;
  private String lastColumn;
  private int rowCount;

  public GoogleSheetsRangeBuilder(String sheetName, int batchSize) {
    this.sheetName = sheetName;
    this.batchSize = batchSize;
    columns = new ArrayList<>();
    batchIndex = -1;
    limit = 0;
    hasMore = true;
    rowCount = 0;
    isStarQuery = false;
  }

  public GoogleSheetsRangeBuilder addColumn(String columnLetter) {
    columns.add(columnLetter);
    return this;
  }

  public GoogleSheetsRangeBuilder addColumn(int columnIndex) {
    columns.add(GoogleSheetsUtils.columnToLetter(columnIndex));
    return this;
  }

  /**
   * Adds a limit to the range builder.
   * @param limit The maximum number of total results returned
   * @return The range builder with the limit applied
   */
  public GoogleSheetsRangeBuilder addLimit(int limit) {
    this.limit = limit;

    return this;
  }

  public GoogleSheetsRangeBuilder addFirstColumn(String column) {
    this.firstColumn = column;
    return this;
  }

  public GoogleSheetsRangeBuilder addLastColumn(String column) {
    this.lastColumn = column;
    return this;
  }

  public GoogleSheetsRangeBuilder addRowCount(int rowCount) {
    this.rowCount = rowCount;
    return this;
  }

  public GoogleSheetsRangeBuilder addProjectedRanges(List<GoogleSheetsColumnRange> projectedRanges) {
    this.projectedRanges = projectedRanges;
    this.isStarQuery = false;
    return this;
  }

  public GoogleSheetsRangeBuilder isStarQuery(boolean isStarQuery) {
    this.isStarQuery = isStarQuery;
    return this;
  }

  private int getStartIndex() {
    return (batchIndex * batchSize) + 1;
  }

  private int getEndIndex() {
    // We have a few cases here:
    int end;
    if (limit == 0 && rowCount == 0) {
      // Case 1.  We have no limit or exact row count
      return (batchIndex + 1) * batchSize;
    } else if (limit > 0 && rowCount == 0) {
      // Case 2:  We have a limit but no exact row count
      end = Math.min(((batchIndex + 1) * batchSize), limit);
    } else if (rowCount > 0 && limit == 0) {
      // Case 3:  We have a rowCount but no limit
      end = Math.min(((batchIndex + 1) * batchSize), rowCount);
    } else {
      // We have both a rowCount and a limit
      end = Math.min(((batchIndex + 1) * batchSize), rowCount);
      end = Math.min(end, limit);
    }
    return end;
  }

  /**
   * When a limit is not present, the BatchReader must call this method
   * to indicate when there are no more results and to stop generating new
   * ranges.
   */
  public void lastBatch() {
    hasMore = false;
  }

  private String build() {
    if (!hasMore) {
      return null;
    } else if (getStartIndex() > getEndIndex()) {
      hasMore = false;
      return null;
    }

    StringBuilder range = new StringBuilder();
    // In the case of a star query, without columns provided all columns are projected.
    // In this case, the range is <SheetName>!>StartIndex:EndIndex
    if ((columns.size() == 0 || isStarQuery) &&
      projectedRanges == null &&
      StringUtils.isEmpty(firstColumn) &&
      StringUtils.isEmpty(lastColumn)) {
      range.append("'")
        .append(sheetName)
        .append("'!")
        .append(getStartIndex())
        .append(":")
        .append(getEndIndex());
    } else if (columns.size() == 0 && isStarQuery) {
      range.append("'")
        .append(sheetName)
        .append("'!")
        .append(firstColumn)
        .append(getStartIndex())
        .append(":")
        .append(lastColumn)
        .append(getEndIndex());
    } else if (projectedRanges != null && projectedRanges.size() > 0) {
      range.append("'")
        .append(sheetName)
        .append("'!");
      int rangeCount = 0;
      for (GoogleSheetsColumnRange columnRange : projectedRanges) {
        if (rangeCount > 0) {
          range.append(",");
        }
        range.append(columnRange.getStartColumnLetter())
          .append(getStartIndex())
          .append(":")
          .append(columnRange.getEndColumnLetter())
          .append(getEndIndex());
        rangeCount++;
      }
    }

    logger.debug("Range built: {}", range);
    return range.toString();
  }

  private List<String> buildBatchList() {
    if (isStarQuery) {
      return null;
    }

    List<String> batchList = new ArrayList<>();
    StringBuilder batch = new StringBuilder();

    for (GoogleSheetsColumnRange columnRange : projectedRanges) {
      batch.append("'")
        .append(sheetName)
        .append("'!")
        .append(columnRange.getStartColumnLetter())
        .append(getStartIndex())
        .append(":")
        .append(columnRange.getEndColumnLetter())
        .append(getEndIndex());
      batchList.add(batch.toString());
      batch = new StringBuilder();
    }
    return batchList;
  }

  @Override
  public boolean hasNext() {
    return hasMore;
  }

  @Override
  public String next() {
    batchIndex++;
    return build();
  }

  public List<String> nextBatch() {
    batchIndex++;
    return buildBatchList();
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("Sheet name", sheetName)
      .field("Batch size", batchSize)
      .field("Limit", limit)
      .field("isStarQuery", isStarQuery)
      .field("First Column", firstColumn)
      .field("Last Column", lastColumn)
      .field("Row Count", rowCount)
      .toString();
  }
}
