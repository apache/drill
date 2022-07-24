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

package org.apache.drill.exec.store.googlesheets.columns;

import org.apache.commons.lang3.StringUtils;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

public abstract class GoogleSheetsColumnWriter {
  protected static final Logger logger = LoggerFactory.getLogger(GoogleSheetsColumnWriter.class);
  protected final RowSetLoader rowWriter;
  protected final ScalarWriter columnWriter;

  public GoogleSheetsColumnWriter(RowSetLoader rowWriter, String colName) {
    this.rowWriter = rowWriter;
    this.columnWriter = rowWriter.scalar(colName);
  }

  public abstract void load(Object value);

  public static class GoogleSheetsBigIntegerColumnWriter extends GoogleSheetsColumnWriter {
    public GoogleSheetsBigIntegerColumnWriter(RowSetLoader rowWriter, String colName) {
      super(rowWriter, colName);
    }

    @Override
    public void load(Object rawValue) {
      String stringValue = (String) rawValue;
      if (StringUtils.isNotEmpty(stringValue)) {
        long finalValue;
        try {
          finalValue = Long.parseLong(stringValue);
          columnWriter.setLong(finalValue);
        } catch (NumberFormatException e) {
          logger.info("Could not parse {} into long from Googlesheets.", stringValue);
        }
      }
    }
  }

  public static class GoogleSheetsBooleanColumnWriter extends GoogleSheetsColumnWriter {

    public GoogleSheetsBooleanColumnWriter(RowSetLoader rowWriter, String colName) {
      super(rowWriter, colName);
    }

    @Override
    public void load(Object rawValue) {
      String stringValue = (String)rawValue;

      if (StringUtils.isNotEmpty(stringValue)) {
        boolean finalValue;
        try {
          finalValue = cleanUpIncomingString(stringValue);
          columnWriter.setBoolean(finalValue);
        } catch (NumberFormatException e) {
          logger.info("Could not parse {} into long from Googlesheets.", stringValue);
        }
      }
    }

    private boolean cleanUpIncomingString(String incoming) {
      if (StringUtils.isEmpty(incoming)) {
        return false;
      } else if (incoming.equalsIgnoreCase("1")) {
        return true;
      } else if (incoming.equalsIgnoreCase("0")) {
        return false;
      } else {
        return Boolean.parseBoolean(incoming);
      }
    }
  }

  public static class GoogleSheetsDateColumnWriter extends GoogleSheetsColumnWriter {

    public GoogleSheetsDateColumnWriter(RowSetLoader rowWriter, String colName) {
      super(rowWriter, colName);
    }

    @Override
    public void load(Object rawValue) {
      String stringValue = (String)rawValue;
      if (StringUtils.isNotEmpty(stringValue)) {
        LocalDate finalValue;
        try {
          finalValue = LocalDate.parse(stringValue);
        } catch (NumberFormatException e) {
          finalValue = null;
        }
        columnWriter.setDate(finalValue);
      }
    }
  }

  public static class GoogleSheetsFloatColumnWriter extends GoogleSheetsColumnWriter {

    public GoogleSheetsFloatColumnWriter(RowSetLoader rowWriter, String colName) {
      super(rowWriter, colName);
    }

    @Override
    public void load(Object rawValue) {
      String stringValue = (String)rawValue;
      if (StringUtils.isNotEmpty(stringValue)) {
        float finalValue;
        try {
          finalValue = Float.parseFloat(stringValue);
        } catch (NumberFormatException e) {
          finalValue = Float.NaN;
        }
        columnWriter.setFloat(finalValue);
      }
    }
  }

  public static class GoogleSheetsIntegerColumnWriter extends GoogleSheetsColumnWriter {

    public GoogleSheetsIntegerColumnWriter(RowSetLoader rowWriter, String colName) {
      super(rowWriter, colName);
    }

    @Override
    public void load(Object rawValue) {
      String stringValue = (String)rawValue;
      if (StringUtils.isNotEmpty(stringValue)) {
        int finalValue;
        try {
          finalValue = (int) Math.floor(Double.parseDouble(stringValue));
          columnWriter.setInt(finalValue);
        } catch (NumberFormatException e) {
          logger.info("Could not parse {} into integer from Googlesheets.", stringValue);
        }
      }
    }
  }

  public static class GoogleSheetsNumericColumnWriter extends GoogleSheetsColumnWriter {

    public GoogleSheetsNumericColumnWriter(RowSetLoader rowWriter, String colName) {
      super(rowWriter, colName);
    }

    @Override
    public void load(Object rawValue) {
      String stringValue = (String)rawValue;
      if (StringUtils.isNotEmpty(stringValue)) {
        double finalValue;
        try {
          finalValue = Double.parseDouble(stringValue);
        } catch (NumberFormatException e) {
          finalValue = Double.NaN;
        }
        columnWriter.setDouble(finalValue);
      }
    }
  }

  public static class GoogleSheetsTimeColumnWriter extends GoogleSheetsColumnWriter {

    public GoogleSheetsTimeColumnWriter(RowSetLoader rowWriter, String colName) {
      super(rowWriter, colName);
    }

    @Override
    public void load(Object rawValue) {
      String stringValue = (String)rawValue;
      if (StringUtils.isNotEmpty(stringValue)) {
        LocalTime finalValue;
        try {
          finalValue = LocalTime.parse(stringValue);
        } catch (NumberFormatException e) {
          finalValue = null;
        }
        columnWriter.setTime(finalValue);
      }
    }
  }

  public static class GoogleSheetsTimestampColumnWriter extends GoogleSheetsColumnWriter {

    public GoogleSheetsTimestampColumnWriter(RowSetLoader rowWriter, String colName) {
      super(rowWriter, colName);
    }

    @Override
    public void load(Object rawValue) {
      String stringValue = (String)rawValue;
      if (StringUtils.isNotEmpty(stringValue)) {
        Instant finalValue;
        try {
          finalValue = Instant.parse(stringValue);
        } catch (NumberFormatException e) {
          finalValue = null;
        }
        columnWriter.setTimestamp(finalValue);
      }
    }
  }

  public static class GoogleSheetsVarcharColumnWriter extends GoogleSheetsColumnWriter {

    public GoogleSheetsVarcharColumnWriter(RowSetLoader rowWriter, String colName) {
      super(rowWriter, colName);
    }

    @Override
    public void load(Object rawValue) {
      String stringValue = (String)rawValue;
      if (StringUtils.isNotEmpty(stringValue)) {
        columnWriter.setString(stringValue);
      }
    }
  }
}
