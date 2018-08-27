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
package org.apache.drill.exec.store.easy.text.compliant;

import org.apache.drill.exec.store.easy.text.TextFormatPlugin.TextFormatConfig;

import org.apache.drill.shaded.guava.com.google.common.base.Charsets;

public class TextParsingSettings {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TextParsingSettings.class);

  public static final TextParsingSettings DEFAULT = new TextParsingSettings();

  private String emptyValue = null;
  private boolean parseUnescapedQuotes = true;
  private byte quote = b('"');
  private byte quoteEscape = b('"');
  private byte delimiter = b(',');
  private byte comment = b('#');

  private long maxCharsPerColumn = Character.MAX_VALUE;
  private byte normalizedNewLine = b('\n');
  private byte[] newLineDelimiter = {normalizedNewLine};
  private boolean ignoreLeadingWhitespaces = false;
  private boolean ignoreTrailingWhitespaces = false;
  private String lineSeparatorString = "\n";
  private boolean skipFirstLine = false;

  private boolean headerExtractionEnabled = false;
  private boolean useRepeatedVarChar = true;
  private int numberOfRecordsToRead = -1;

  public void set(TextFormatConfig config){
    this.quote = bSafe(config.getQuote(), "quote");
    this.quoteEscape = bSafe(config.getEscape(), "escape");
    this.newLineDelimiter = config.getLineDelimiter().getBytes(Charsets.UTF_8);
    this.delimiter = bSafe(config.getFieldDelimiter(), "fieldDelimiter");
    this.comment = bSafe(config.getComment(), "comment");
    this.skipFirstLine = config.isSkipFirstLine();
    this.headerExtractionEnabled = config.isHeaderExtractionEnabled();
    if (this.headerExtractionEnabled) {
      // In case of header TextRecordReader will use set of VarChar vectors vs RepeatedVarChar
      this.useRepeatedVarChar = false;
    }
  }

  public byte getComment(){
    return comment;
  }

  public boolean isSkipFirstLine() {
    return skipFirstLine;
  }

  public void setSkipFirstLine(boolean skipFirstLine) {
    this.skipFirstLine = skipFirstLine;
  }

  public boolean isUseRepeatedVarChar() {
    return useRepeatedVarChar;
  }

  public void setUseRepeatedVarChar(boolean useRepeatedVarChar) {
    this.useRepeatedVarChar = useRepeatedVarChar;
  }


  private static byte bSafe(char c, String name){
    if(c > Byte.MAX_VALUE) {
      throw new IllegalArgumentException(String.format("Failure validating configuration option %s.  Expected a "
          + "character between 0 and 127 but value was actually %d.", name, (int) c));
    }
    return (byte) c;
  }

  private static byte b(char c){
    return (byte) c;
  }

  public byte[] getNewLineDelimiter() {
    return newLineDelimiter;
  }

  /**
   * Returns the character used for escaping values where the field delimiter is part of the value. Defaults to '"'
   * @return the quote character
   */
  public byte getQuote() {
    return quote;
  }

  /**
   * Defines the character used for escaping values where the field delimiter is part of the value. Defaults to '"'
   * @param quote the quote character
   */
  public void setQuote(byte quote) {
    this.quote = quote;
  }

  public String getLineSeparatorString(){
    return lineSeparatorString;
  }


  /**
   * Identifies whether or not a given character is used for escaping values where the field delimiter is part of the value
   * @param ch the character to be verified
   * @return true if the given character is the character used for escaping values, false otherwise
   */
  public boolean isQuote(byte ch) {
    return this.quote == ch;
  }

  /**
   * Returns the character used for escaping quotes inside an already quoted value. Defaults to '"'
   * @return the quote escape character
   */
  public byte getQuoteEscape() {
    return quoteEscape;
  }

  /**
   * Defines the character used for escaping quotes inside an already quoted value. Defaults to '"'
   * @param quoteEscape the quote escape character
   */
  public void setQuoteEscape(byte quoteEscape) {
    this.quoteEscape = quoteEscape;
  }

  /**
   * Identifies whether or not a given character is used for escaping quotes inside an already quoted value.
   * @param ch the character to be verified
   * @return true if the given character is the quote escape character, false otherwise
   */
  public boolean isQuoteEscape(byte ch) {
    return this.quoteEscape == ch;
  }

  /**
   * Returns the field delimiter character. Defaults to ','
   * @return the field delimiter character
   */
  public byte getDelimiter() {
    return delimiter;
  }

  /**
   * Defines the field delimiter character. Defaults to ','
   * @param delimiter the field delimiter character
   */
  public void setDelimiter(byte delimiter) {
    this.delimiter = delimiter;
  }

  /**
   * Identifies whether or not a given character represents a field delimiter
   * @param ch the character to be verified
   * @return true if the given character is the field delimiter character, false otherwise
   */
  public boolean isDelimiter(byte ch) {
    return this.delimiter == ch;
  }

  /**
   * Returns the String representation of an empty value (defaults to null)
   *
   * <p>When reading, if the parser does not read any character from the input, and the input is within quotes, the empty is used instead of an empty string
   *
   * @return the String representation of an empty value
   */
  public String getEmptyValue() {
    return emptyValue;
  }

  /**
   * Sets the String representation of an empty value (defaults to null)
   *
   * <p>When reading, if the parser does not read any character from the input, and the input is within quotes, the empty is used instead of an empty string
   *
   * @param emptyValue the String representation of an empty value
   */
  public void setEmptyValue(String emptyValue) {
    this.emptyValue = emptyValue;
  }


  /**
   * Indicates whether the CSV parser should accept unescaped quotes inside quoted values and parse them normally. Defaults to {@code true}.
   * @return a flag indicating whether or not the CSV parser should accept unescaped quotes inside quoted values.
   */
  public boolean isParseUnescapedQuotes() {
    return parseUnescapedQuotes;
  }

  /**
   * Configures how to handle unescaped quotes inside quoted values. If set to {@code true}, the parser will parse the quote normally as part of the value.
   * If set the {@code false}, a {@link com.univocity.parsers.common.TextParsingException} will be thrown. Defaults to {@code true}.
   * @param parseUnescapedQuotes indicates whether or not the CSV parser should accept unescaped quotes inside quoted values.
   */
  public void setParseUnescapedQuotes(boolean parseUnescapedQuotes) {
    this.parseUnescapedQuotes = parseUnescapedQuotes;
  }

  /**
   * Indicates whether or not the first valid record parsed from the input should be considered as the row containing the names of each column
   * @return true if the first valid record parsed from the input should be considered as the row containing the names of each column, false otherwise
   */
  public boolean isHeaderExtractionEnabled() {
    return headerExtractionEnabled;
  }

  /**
   * Defines whether or not the first valid record parsed from the input should be considered as the row containing the names of each column
   * @param headerExtractionEnabled a flag indicating whether the first valid record parsed from the input should be considered as the row containing the names of each column
   */
  public void setHeaderExtractionEnabled(boolean headerExtractionEnabled) {
    this.headerExtractionEnabled = headerExtractionEnabled;
  }

  /**
   * The number of valid records to be parsed before the process is stopped. A negative value indicates there's no limit (defaults to -1).
   * @return the number of records to read before stopping the parsing process.
   */
  public int getNumberOfRecordsToRead() {
    return numberOfRecordsToRead;
  }

  /**
   * Defines the number of valid records to be parsed before the process is stopped. A negative value indicates there's no limit (defaults to -1).
   * @param numberOfRecordsToRead the number of records to read before stopping the parsing process.
   */
  public void setNumberOfRecordsToRead(int numberOfRecordsToRead) {
    this.numberOfRecordsToRead = numberOfRecordsToRead;
  }

  public long getMaxCharsPerColumn() {
    return maxCharsPerColumn;
  }

  public void setMaxCharsPerColumn(long maxCharsPerColumn) {
    this.maxCharsPerColumn = maxCharsPerColumn;
  }

  public void setComment(byte comment) {
    this.comment = comment;
  }

  public byte getNormalizedNewLine() {
    return normalizedNewLine;
  }

  public void setNormalizedNewLine(byte normalizedNewLine) {
    this.normalizedNewLine = normalizedNewLine;
  }

  public boolean isIgnoreLeadingWhitespaces() {
    return ignoreLeadingWhitespaces;
  }

  public void setIgnoreLeadingWhitespaces(boolean ignoreLeadingWhitespaces) {
    this.ignoreLeadingWhitespaces = ignoreLeadingWhitespaces;
  }

  public boolean isIgnoreTrailingWhitespaces() {
    return ignoreTrailingWhitespaces;
  }

  public void setIgnoreTrailingWhitespaces(boolean ignoreTrailingWhitespaces) {
    this.ignoreTrailingWhitespaces = ignoreTrailingWhitespaces;
  }





}
