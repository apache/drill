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

package org.apache.drill.exec.store.http;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.drill.common.PlanStringBuilder;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonDeserialize(builder = HttpCSVOptions.HttpCSVOptionsBuilder.class)
public class HttpCSVOptions {


  @JsonProperty
  private final String delimiter;

  @JsonProperty
  private final char quote;

  @JsonProperty
  private final char quoteEscape;

  @JsonProperty
  private final String lineSeparator;

  @JsonProperty
  private final Boolean headerExtractionEnabled;

  @JsonProperty
  private final long numberOfRowsToSkip;

  @JsonProperty
  private final long numberOfRecordsToRead;

  @JsonProperty
  private final boolean lineSeparatorDetectionEnabled;

  @JsonProperty
  private final int maxColumns;

  @JsonProperty
  private final int maxCharsPerColumn;

  @JsonProperty
  private final boolean skipEmptyLines;

  @JsonProperty
  private final boolean ignoreLeadingWhitespaces;

  @JsonProperty
  private final boolean ignoreTrailingWhitespaces;

  @JsonProperty
  private final String nullValue;

  HttpCSVOptions(HttpCSVOptionsBuilder builder) {
    this.delimiter = builder.delimiter;
    this.quote = builder.quote;
    this.quoteEscape = builder.quoteEscape;
    this.lineSeparator = builder.lineSeparator;
    this.headerExtractionEnabled = builder.headerExtractionEnabled;
    this.numberOfRowsToSkip = builder.numberOfRowsToSkip;
    this.numberOfRecordsToRead = builder.numberOfRecordsToRead;
    this.lineSeparatorDetectionEnabled = builder.lineSeparatorDetectionEnabled;
    this.maxColumns = builder.maxColumns;
    this.maxCharsPerColumn = builder.maxCharsPerColumn;
    this.skipEmptyLines = builder.skipEmptyLines;
    this.ignoreLeadingWhitespaces = builder.ignoreLeadingWhitespaces;
    this.ignoreTrailingWhitespaces = builder.ignoreTrailingWhitespaces;
    this.nullValue = builder.nullValue;
  }

  public static HttpCSVOptionsBuilder builder() {
    return new HttpCSVOptionsBuilder();
  }

  public String getDelimiter() {
    return delimiter;
  }

  public char getQuote() {
    return quote;
  }

  public char getQuoteEscape() {
    return quoteEscape;
  }

  public String getLineSeparator() {
    return lineSeparator;
  }

  public Boolean getHeaderExtractionEnabled() {
    return headerExtractionEnabled;
  }

  public long getNumberOfRowsToSkip() {
    return numberOfRowsToSkip;
  }

  public long getNumberOfRecordsToRead() {
    return numberOfRecordsToRead;
  }

  public boolean isLineSeparatorDetectionEnabled() {
    return lineSeparatorDetectionEnabled;
  }

  public int getMaxColumns() {
    return maxColumns;
  }

  public int getMaxCharsPerColumn() {
    return maxCharsPerColumn;
  }

  public boolean isSkipEmptyLines() {
    return skipEmptyLines;
  }

  public boolean isIgnoreLeadingWhitespaces() {
    return ignoreLeadingWhitespaces;
  }

  public boolean isIgnoreTrailingWhitespaces() {
    return ignoreTrailingWhitespaces;
  }

  public String getNullValue() {
    return nullValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HttpCSVOptions that = (HttpCSVOptions) o;
    return quote == that.quote
        && quoteEscape == that.quoteEscape
        && numberOfRowsToSkip == that.numberOfRowsToSkip
        && numberOfRecordsToRead == that.numberOfRecordsToRead
        && lineSeparatorDetectionEnabled == that.lineSeparatorDetectionEnabled
        && maxColumns == that.maxColumns && maxCharsPerColumn == that.maxCharsPerColumn
        && skipEmptyLines == that.skipEmptyLines
        && ignoreLeadingWhitespaces == that.ignoreLeadingWhitespaces
        && ignoreTrailingWhitespaces == that.ignoreTrailingWhitespaces
        && delimiter.equals(that.delimiter)
        && lineSeparator.equals(that.lineSeparator)
        && Objects.equals(headerExtractionEnabled, that.headerExtractionEnabled)
        && nullValue.equals(that.nullValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(delimiter, quote, quoteEscape, lineSeparator, headerExtractionEnabled,
        numberOfRowsToSkip, numberOfRecordsToRead, lineSeparatorDetectionEnabled, maxColumns,
        maxCharsPerColumn, skipEmptyLines, ignoreLeadingWhitespaces, ignoreTrailingWhitespaces,
        nullValue);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
        .field("delimiter", delimiter)
        .field("quote", quote)
        .field("quoteEscape", quoteEscape)
        .field("lineSeparator", lineSeparator)
        .field("headerExtractionEnabled", headerExtractionEnabled)
        .field("numberOfRowsToSkip", numberOfRowsToSkip)
        .field("numberOfRecordsToRead", numberOfRecordsToRead)
        .field("lineSeparatorDetectionEnabled", lineSeparatorDetectionEnabled)
        .field("maxColumns", maxColumns)
        .field("maxCharsPerColumn", maxCharsPerColumn)
        .field("skipEmptyLines", skipEmptyLines)
        .field("ignoreLeadingWhitespaces", ignoreLeadingWhitespaces)
        .field("ignoreTrailingWhitespaces", ignoreTrailingWhitespaces)
        .field("nullValue", nullValue)
        .toString();
  }


  @JsonPOJOBuilder(withPrefix = "")
  public static class HttpCSVOptionsBuilder {
    private String delimiter = ",";
    private char quote = '"';

    private char quoteEscape = '"';

    private String lineSeparator = "\n";

    private Boolean headerExtractionEnabled = null;

    private long numberOfRowsToSkip = 0;

    private long numberOfRecordsToRead = -1;

    private boolean lineSeparatorDetectionEnabled = true;

    private int maxColumns = 512;

    private int maxCharsPerColumn = 4096;

    private boolean skipEmptyLines = true;

    private boolean ignoreLeadingWhitespaces = true;

    private boolean ignoreTrailingWhitespaces = true;

    private String nullValue = null;


    public HttpCSVOptionsBuilder delimiter(String delimiter) {
      this.delimiter = delimiter;
      return this;
    }

    public HttpCSVOptionsBuilder quote(char quote) {
      this.quote = quote;
      return this;
    }

    public HttpCSVOptionsBuilder quoteEscape(char quoteEscape) {
      this.quoteEscape = quoteEscape;
      return this;
    }

    public HttpCSVOptionsBuilder lineSeparator(String lineSeparator) {
      this.lineSeparator = lineSeparator;
      return this;
    }

    public HttpCSVOptionsBuilder headerExtractionEnabled(Boolean headerExtractionEnabled) {
      this.headerExtractionEnabled = headerExtractionEnabled;
      return this;
    }

    public HttpCSVOptionsBuilder numberOfRowsToSkip(long numberOfRowsToSkip) {
      this.numberOfRowsToSkip = numberOfRowsToSkip;
      return this;
    }

    public HttpCSVOptionsBuilder numberOfRecordsToRead(long numberOfRecordsToRead) {
      this.numberOfRecordsToRead = numberOfRecordsToRead;
      return this;
    }

    public HttpCSVOptionsBuilder lineSeparatorDetectionEnabled(boolean lineSeparatorDetectionEnabled) {
      this.lineSeparatorDetectionEnabled = lineSeparatorDetectionEnabled;
      return this;
    }

    public HttpCSVOptionsBuilder maxColumns(int maxColumns) {
      this.maxColumns = maxColumns;
      return this;
    }

    public HttpCSVOptionsBuilder maxCharsPerColumn(int maxCharsPerColumn) {
      this.maxCharsPerColumn = maxCharsPerColumn;
      return this;
    }

    public HttpCSVOptionsBuilder skipEmptyLines(boolean skipEmptyLines) {
      this.skipEmptyLines = skipEmptyLines;
      return this;
    }

    public HttpCSVOptionsBuilder ignoreLeadingWhitespaces(boolean ignoreLeadingWhitespaces) {
      this.ignoreLeadingWhitespaces = ignoreLeadingWhitespaces;
      return this;
    }

    public HttpCSVOptionsBuilder ignoreTrailingWhitespaces(boolean ignoreTrailingWhitespaces) {
      this.ignoreTrailingWhitespaces = ignoreTrailingWhitespaces;
      return this;
    }

    public HttpCSVOptionsBuilder nullValue(String nullValue) {
      this.nullValue = nullValue;
      return this;
    }


    public HttpCSVOptions build() {

      return new HttpCSVOptions(this);
    }
  }
}
