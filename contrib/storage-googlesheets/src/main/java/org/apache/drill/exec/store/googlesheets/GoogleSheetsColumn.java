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


import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.store.googlesheets.columns.GoogleSheetsColumnWriter;
import org.apache.drill.exec.store.googlesheets.utils.GoogleSheetsUtils;
import org.apache.drill.exec.store.googlesheets.utils.GoogleSheetsUtils.DATA_TYPES;

import java.util.Objects;


/**
 * This class is the representation of a GoogleSheets column. Since column
 * metadata appears to be unavailable at either by accessing the master document
 * or the sheet itself, this class gathers it and represents that as a Java object.
 *
 * Additionally, GoogleSheets does not allow you to name columns or access them by position,
 * instead using A1 notation (or other equally useless forms of accessing columns). In order to facilitate
 * the projection pushdown we have to track the column's: name, data type and also be able
 * to translate that position into a letter.  Note that Google sheets has a limit of approx 18k
 * columns.
 */
public class GoogleSheetsColumn {
  private final String columnName;
  private final GoogleSheetsUtils.DATA_TYPES dataType;
  private final MinorType drillDataType;
  private final DataMode dataMode;
  private final int columnIndex;
  private final int drillColumnIndex;
  private final String columnLetter;
  private final boolean isMetadata;

  private GoogleSheetsColumnWriter writer;

  public GoogleSheetsColumn(String columnName, GoogleSheetsUtils.DATA_TYPES dataType, int googleColumnIndex, int drillColumnIndex) {
    this.columnName = columnName;
    this.columnIndex = googleColumnIndex;
    this.drillColumnIndex = drillColumnIndex;
    this.dataType = dataType;
    this.columnLetter = GoogleSheetsUtils.columnToLetter(googleColumnIndex + 1);
    this.drillDataType = getDrillDataType(dataType);
    this.dataMode = DataMode.OPTIONAL;
    this.isMetadata = false;
  }

  public GoogleSheetsColumn(String columnName, GoogleSheetsUtils.DATA_TYPES dataType, int drillColumnIndex, boolean isMetadata) {
    // Constructor for metadata fields.
    this.columnName = columnName;
    this.columnIndex = -1;
    this.drillColumnIndex = drillColumnIndex;
    this.dataType = dataType;
    this.columnLetter = null;
    this.drillDataType = getDrillDataType(dataType);
    if (dataType == DATA_TYPES.VARCHAR_REPEATED) {
      dataMode = DataMode.REPEATED;
    } else {
      dataMode = DataMode.OPTIONAL;
    }
    this.isMetadata = isMetadata;
  }

  private MinorType getDrillDataType(GoogleSheetsUtils.DATA_TYPES dataType) {
    switch (dataType) {
      case NUMERIC:
        return MinorType.FLOAT8;
      case DATE:
        return MinorType.DATE;
      case TIME:
        return MinorType.TIME;
      case TIMESTAMP:
        return MinorType.TIMESTAMP;
      case UNKNOWN:
      case VARCHAR:
      default:
        return MinorType.VARCHAR;
    }
  }

  public void setWriter(GoogleSheetsColumnWriter writer) {
    this.writer = writer;
  }

  public MinorType getDrillDataType() {
    return drillDataType;
  }

  public int getColumnIndex() {
    return columnIndex;
  }

  public int getDrillColumnIndex() { return drillColumnIndex; }

  public String getColumnLetter() { return columnLetter; }

  public boolean isMetadata() {
    return this.isMetadata;
  }

  public String getColumnName() {
    return columnName;
  }

  public void load(Object value) {
    if (! isMetadata) {
      writer.load(value);
    }
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("columnName", columnName)
      .field("columnIndex", columnIndex)
      .field("columnLetter", columnLetter)
      .field("data type", dataType)
      .field("isMetadata", isMetadata)
      .toString();
  }
  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    } else if (that == null || getClass() != that.getClass()) {
      return false;
    }
    GoogleSheetsColumn otherColumn  = (GoogleSheetsColumn) that;
    return Objects.equals(columnName, otherColumn.columnName) &&
      Objects.equals(columnIndex, otherColumn.columnIndex) &&
      Objects.equals(columnLetter, otherColumn.columnLetter) &&
      Objects.equals(dataType, otherColumn.dataType) &&
      Objects.equals(isMetadata, otherColumn.isMetadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnName, columnIndex, columnLetter, dataType, isMetadata);
  }
}
