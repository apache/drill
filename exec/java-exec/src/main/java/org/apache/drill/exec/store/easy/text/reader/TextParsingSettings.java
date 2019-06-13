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
package org.apache.drill.exec.store.easy.text.reader;

import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.drill.exec.store.easy.text.TextFormatPlugin;
import org.apache.drill.exec.store.easy.text.TextFormatPlugin.TextFormatConfig;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;

public class TextParsingSettings {

  private final String emptyValue = null;
  private final boolean parseUnescapedQuotes = true;
  private final byte quote;
  private final byte quoteEscape;
  private final byte delimiter;
  private final byte comment;

  private final long maxCharsPerColumn = Character.MAX_VALUE;
  private final byte normalizedNewLine = b('\n');
  private final byte[] newLineDelimiter;
  private final boolean ignoreLeadingWhitespaces = false;
  private final boolean ignoreTrailingWhitespaces = false;
  private final String lineSeparatorString = "\n";
  private boolean skipFirstLine;

  private final boolean headerExtractionEnabled;
  private final boolean useRepeatedVarChar;
  private final String providedHeaders[];

  /**
   * Configure the properties for this one scan based on:
   * <p>
   * <ul>
   * <li>The defaults in the plugin config (if properties not defined
   * in the config JSON.</li>
   * <li>The config values from the config JSON as stored in the
   * plugin config.</li>
   * <li>Table function settings expressed in the query (and passed
   * in as part of the plugin config.</li>
   * <li>Table properties.</li>
   * </ul>
   * <p>
   * The result is that the user can customize the behavior of a table just
   * via the table properties; the user need not define a new storage
   * config just to change a property. For example, by default, the
   * <tt>`csv`</tt> config has no headers. But, if the user has a ".csv"
   * file with headers, the user can just customize the table properties.
   */
  public TextParsingSettings(TextFormatConfig config,
      EasySubScan scan, OptionManager options) {
    TupleMetadata providedSchema = scan.getSchema();
    boolean extractHeaders = config.isHeaderExtractionEnabled();
    boolean skipFirst = config.isSkipFirstLine();
    String providedHeaders[] = null;
    byte delimChar = bSafe(config.getFieldDelimiter(), "fieldDelimiter");
    byte commentChar = bSafe(config.getComment(), "comment");
    byte quoteChar = bSafe(config.getQuote(), "quote");
    byte quoteEscapeChar = bSafe(config.getEscape(), "escape");
    byte[] newlineDelim = config.getLineDelimiter().getBytes(Charsets.UTF_8);
    if (providedSchema != null) {
      extractHeaders = providedSchema.booleanProperty(
          TextFormatPlugin.HAS_HEADERS_PROP, extractHeaders);
      skipFirst = ! extractHeaders & providedSchema.booleanProperty(
          TextFormatPlugin.SKIP_FIRST_LINE_PROP, skipFirstLine);
      if (!extractHeaders && ! providedSchema.isEmpty()) {
        providedHeaders = new String[providedSchema.size()];
        for (int i = 0; i < providedHeaders.length; i++) {
          providedHeaders[i] = providedSchema.metadata(i).name();
        }
      }
      delimChar = overrideChar(providedSchema, TextFormatPlugin.DELIMITER_PROP, delimChar);
      quoteChar = overrideChar(providedSchema, TextFormatPlugin.QUOTE_PROP, quoteChar);
      quoteEscapeChar = overrideChar(providedSchema, TextFormatPlugin.QUOTE_ESCAPE_PROP, quoteEscapeChar);
      newlineDelim = newlineDelimBytes(providedSchema, newlineDelim);
      commentChar = commentChar(providedSchema, commentChar);
    }
    skipFirstLine = !extractHeaders && skipFirst;
    headerExtractionEnabled = extractHeaders;
    this.providedHeaders = providedHeaders;
    useRepeatedVarChar = !extractHeaders && providedHeaders == null;

    quote = quoteChar;
    quoteEscape = quoteEscapeChar;
    newLineDelimiter = newlineDelim;
    delimiter = delimChar;
    comment = commentChar;
  }

  /**
   * Parse a delimiter from table properties. If the property is unset,
   * or is a blank string, then uses the delimiter from the plugin config.
   * Else, if non-blank, uses the first character of the property value.
   */

  private static byte overrideChar(TupleMetadata providedSchema, String propName, byte configValue) {
    String value = providedSchema.property(propName);
    if (value == null || value.isEmpty()) {
      return configValue;
    }
    // Text reader supports only ASCII text and characters.
    return (byte) value.charAt(0);
  }

  /**
   * Parse a comment character from table properties. If the property is unset,
   * then uses the delimiter from the plugin config. If the properry value is
   * blank, then uses ASCII NUL (0) as the comment. This value should never
   * match anything, and effectively disables the comment feature.
   * Else, if non-blank, uses the first character of the property value.
   */

  private static byte commentChar(TupleMetadata providedSchema, byte configValue) {
    String value = providedSchema.property(TextFormatPlugin.COMMENT_CHAR_PROP);
    if (value == null) {
      return configValue;
    }
    if (value.isEmpty()) {
      return 0;
    }
    // Text reader supports only ASCII text and characters.
    return (byte) value.charAt(0);
  }

  /**
   * Return either line delimiter from table properties, or the one
   * provided as a parameter from the plugin config. The line delimiter
   * can contain multiple bytes.
   */
  private static byte[] newlineDelimBytes(TupleMetadata providedSchema, byte[] configValue) {
    String value = providedSchema.property(TextFormatPlugin.LINE_DELIM_PROP);
    if (value == null || value.isEmpty()) {
      return configValue;
    }
    return value.getBytes();
  }

  public byte getComment() {
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

  private static byte bSafe(char c, String name) {
    if (c > Byte.MAX_VALUE) {
      throw new IllegalArgumentException(String.format("Failure validating configuration option %s.  Expected a "
          + "character between 0 and 127 but value was actually %d.", name, (int) c));
    }
    return (byte) c;
  }

  private static byte b(char c) {
    return (byte) c;
  }

  public byte[] getNewLineDelimiter() {
    return newLineDelimiter;
  }

  /**
   * Returns the character used for escaping values where the field delimiter is
   * part of the value. Defaults to '"'
   *
   * @return the quote character
   */
  public byte getQuote() {
    return quote;
  }

  public String getLineSeparatorString() {
    return lineSeparatorString;
  }

  /**
   * Returns the character used for escaping quotes inside an already quoted value. Defaults to '"'
   * @return the quote escape character
   */
  public byte getQuoteEscape() {
    return quoteEscape;
  }

  /**
   * Returns the field delimiter character. Defaults to ','
   * @return the field delimiter character
   */
  public byte getDelimiter() {
    return delimiter;
  }

  /**
   * Returns the String representation of an empty value (defaults to null)
   *
   * <p>
   * When reading, if the parser does not read any character from the input, and
   * the input is within quotes, the empty is used instead of an empty string
   *
   * @return the String representation of an empty value
   */
  public String getEmptyValue() {
    return emptyValue;
  }

  /**
   * Indicates whether the CSV parser should accept unescaped quotes inside
   * quoted values and parse them normally. Defaults to {@code true}.
   *
   * @return a flag indicating whether or not the CSV parser should accept
   *         unescaped quotes inside quoted values.
   */
  public boolean isParseUnescapedQuotes() {
    return parseUnescapedQuotes;
  }

  /**
   * Indicates whether or not the first valid record parsed from the input
   * should be considered as the row containing the names of each column
   *
   * @return true if the first valid record parsed from the input should be
   *         considered as the row containing the names of each column, false
   *         otherwise
   */
  public boolean isHeaderExtractionEnabled() {
    return headerExtractionEnabled;
  }

  public long getMaxCharsPerColumn() {
    return maxCharsPerColumn;
  }

  public byte getNormalizedNewLine() {
    return normalizedNewLine;
  }

  public boolean isIgnoreLeadingWhitespaces() {
    return ignoreLeadingWhitespaces;
  }

  public boolean isIgnoreTrailingWhitespaces() {
    return ignoreTrailingWhitespaces;
  }

  public String[] providedHeaders() { return providedHeaders; }
}
