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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.exec.store.excel.ExcelBatchReader.ExcelReaderConfig;
import com.google.common.collect.ImmutableList;
import org.apache.poi.ss.SpreadsheetVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

@JsonTypeName(ExcelFormatPlugin.DEFAULT_NAME)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class ExcelFormatConfig implements FormatPluginConfig {

  private static final Logger logger = LoggerFactory.getLogger(ExcelFormatPlugin.class);

  // This is the theoretical maximum number of rows in an Excel spreadsheet
  private final int MAX_ROWS = SpreadsheetVersion.EXCEL2007.getMaxRows();

  private final List<String> extensions;
  private final int headerRow;
  private final int lastRow;
  private final int firstColumn;
  private final int lastColumn;
  private final boolean allTextMode;
  private final boolean ignoreErrors;
  private final String sheetName;
  private final int maxArraySize;
  private final int thresholdBytesForTempFiles;
  private final boolean useTempFilePackageParts;

  // Omitted properties take reasonable defaults
  @JsonCreator
  public ExcelFormatConfig(
      @JsonProperty("extensions") List<String> extensions,
      @JsonProperty("headerRow") Integer headerRow,
      @JsonProperty("lastRow") Integer lastRow,
      @JsonProperty("firstColumn") Integer firstColumn,
      @JsonProperty("lastColumn") Integer lastColumn,
      @JsonProperty("allTextMode") Boolean allTextMode,
      @JsonProperty("ignoreErrors") Boolean ignoreErrors,
      @JsonProperty("sheetName") String sheetName,
      @JsonProperty("maxArraySize") Integer maxArraySize,
      @JsonProperty("thresholdBytesForTempFiles") Integer thresholdBytesForTempFiles,
      @JsonProperty("useTempFilePackageParts") Boolean useTempFilePackageParts
  ) {
    this.extensions = extensions == null
        ? Collections.singletonList("xlsx")
        : ImmutableList.copyOf(extensions);
    this.headerRow = headerRow == null ? 0 : headerRow;
    this.lastRow = lastRow == null ? MAX_ROWS :
      Math.min(MAX_ROWS, lastRow);
    this.firstColumn = firstColumn == null ? 0 : firstColumn;
    this.lastColumn = lastColumn == null ? 0 : lastColumn;
    this.allTextMode = allTextMode == null ? false : allTextMode;
    this.ignoreErrors = ignoreErrors == null ? true : ignoreErrors;
    this.sheetName = sheetName == null ? "" : sheetName;
    this.maxArraySize = maxArraySize == null ? -1 : maxArraySize;
    this.thresholdBytesForTempFiles = thresholdBytesForTempFiles == null ? -1 : thresholdBytesForTempFiles;
    this.useTempFilePackageParts = useTempFilePackageParts == null ? false : useTempFilePackageParts;
    validateConfig();
  }

  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public List<String> getExtensions() {
    return extensions;
  }

  public int getHeaderRow() {
    return headerRow;
  }

  public int getLastRow() {
    return lastRow;
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
  public boolean getIgnoreErrors() {
    return ignoreErrors;
  }

  public String getSheetName() {
    return sheetName;
  }

  /**
   * See the <code>setByteArrayMaxOverride</code> section in the Apache POI
   * <a href="https://poi.apache.org/components/configuration.html">configuration</a> doc.
   * @return max size of POI array (-1 means default limits applied)
   */
  public int getMaxArraySize() {
    return maxArraySize;
  }

  /**
   * See the <code>setThresholdBytesForTempFiles</code> section in the Apache POI
   * <a href="https://poi.apache.org/components/configuration.html">configuration</a> doc.
   * @return size at which xlsx parts are switched to temp file backing
   * (-1 means no temp files for this feature - POI does use temp files in some other cases)
   */
  public int getThresholdBytesForTempFiles() {
    return thresholdBytesForTempFiles;
  }

  /**
   * See the <code>setUseTempFilePackageParts</code> section in the Apache POI
   * <a href="https://poi.apache.org/components/configuration.html">configuration</a> doc.
   * @return whether to use temp files for package parts (default is false)
   */
  public boolean isUseTempFilePackageParts() {
    return useTempFilePackageParts;
  }

  public ExcelReaderConfig getReaderConfig(ExcelFormatPlugin plugin) {
    ExcelReaderConfig readerConfig = new ExcelReaderConfig(plugin);
    return readerConfig;
  }

  @Override
  public int hashCode() {
    return Objects.hash(extensions, headerRow, lastRow,
        firstColumn, lastColumn, allTextMode, ignoreErrors, sheetName);
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
    return Objects.equals(extensions, other.extensions)
      && Objects.equals(headerRow, other.headerRow)
      && Objects.equals(lastRow, other.lastRow)
      && Objects.equals(firstColumn, other.firstColumn)
      && Objects.equals(lastColumn, other.lastColumn)
      && Objects.equals(allTextMode, other.allTextMode)
      && Objects.equals(ignoreErrors, other.ignoreErrors)
      && Objects.equals(sheetName, other.sheetName)
      && Objects.equals(maxArraySize, other.maxArraySize)
      && Objects.equals(thresholdBytesForTempFiles, other.thresholdBytesForTempFiles)
      && Objects.equals(useTempFilePackageParts, other.useTempFilePackageParts);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
        .field("extensions", extensions)
        .field("sheetName", sheetName)
        .field("headerRow", headerRow)
        .field("lastRow", lastRow)
        .field("firstColumn", firstColumn)
        .field("lastColumn", lastColumn)
        .field("allTextMode", allTextMode)
        .field("ignoreErrors", ignoreErrors)
        .field("maxArraySize", maxArraySize)
        .field("thresholdBytesForTempFiles", thresholdBytesForTempFiles)
        .field("useTempFilePackageParts", useTempFilePackageParts)
        .toString();
  }

  /**
   * This function validates that the user entered valid user configuration options.  Specifically it verifies that:
   * <ul>
   * <li>the lastColumn is greater than the first column</li>
   * <li>The lastColumn is not zero</li>
   * <li>firstColumn is greater than zero</li>
   * <li>lastColumn is greater than zero</li>
   * <li>The headerRow index is less than the lastRow index</li>
   * </ul>
   *
   */
  private void validateConfig() {
    // Validate the config variables
    if ((lastColumn < firstColumn) && lastColumn != 0) {
      throw UserException
        .validationError()
        .message("Invalid column configuration. The first column index is greater than the last column index.")
        .build(logger);
    }

    if (firstColumn < 0) {
      throw UserException
        .validationError()
        .message("Invalid value for first column. Index must be greater than zero.")
        .build(logger);
    }

    if (lastColumn < 0) {
      throw UserException
        .validationError()
        .message("Invalid value for last column. Index must be greater than zero.")
        .build(logger);
    }

    if (headerRow > lastRow) {
      throw UserException
        .validationError()
        .message("Invalid value for headerRow. Header row must be less than last row.")
        .build(logger);
    }
  }
}
