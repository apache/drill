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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.exec.store.excel.ExcelBatchReader.ExcelReaderConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@JsonTypeName(ExcelFormatPlugin.DEFAULT_NAME)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class ExcelFormatConfig implements FormatPluginConfig {

  // This is the theoretical maximum number of rows in an Excel spreadsheet
  private final int MAX_ROWS = 1048576;

  public List<String> extensions = Collections.singletonList("xlsx");

  public int headerRow;

  public int lastRow = MAX_ROWS;

  public int firstColumn;

  public int lastColumn;

  public boolean allTextMode;

  public String sheetName = "";

  public int getHeaderRow() {
    return headerRow;
  }

  public int getLastRow() {
    return lastRow;
  }

  public String getSheetName() {
    return sheetName;
  }

  public int getFirstColumn() {
    return firstColumn;
  }

  public int getLastColumn() {
    return lastColumn;
  }

  public boolean getAllTextMode() {
    return allTextMode;
  }

  public ExcelReaderConfig getReaderConfig(ExcelFormatPlugin plugin) {
    ExcelReaderConfig readerConfig = new ExcelReaderConfig(plugin);
    return readerConfig;
  }

  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public List<String> getExtensions() {
    return extensions;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(
      new Object[]{extensions, headerRow, lastRow, sheetName, firstColumn, lastColumn, allTextMode});
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ExcelFormatConfig other = (ExcelFormatConfig) obj;
    return Objects.equals(headerRow, other.headerRow)
      && Objects.equals(lastRow, other.lastRow)
      && Objects.equals(firstColumn, other.firstColumn)
      && Objects.equals(lastColumn, other.lastColumn)
      && Objects.equals(sheetName, other.sheetName)
      && Objects.equals(allTextMode, other.allTextMode);
  }
}
