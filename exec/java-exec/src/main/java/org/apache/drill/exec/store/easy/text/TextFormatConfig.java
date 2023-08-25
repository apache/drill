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
package org.apache.drill.exec.store.easy.text;

import java.util.List;
import java.util.Objects;

import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.logical.FormatPluginConfig;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonTypeName(TextFormatPlugin.PLUGIN_NAME)
@JsonInclude(Include.NON_DEFAULT)
public class TextFormatConfig implements FormatPluginConfig {

  public final List<String> extensions;
  public final String lineDelimiter;
  public final char fieldDelimiter;
  public final char quote;
  public final char escape;
  public final char comment;
  public final boolean skipFirstLine;
  public final boolean extractHeader;

  @JsonCreator
  public TextFormatConfig(
      @JsonProperty("extensions") List<String> extensions,
      @JsonProperty("lineDelimiter") String lineDelimiter,
      // Drill 1.17 and before used "delimiter" in the
      // bootstrap storage plugins file, assume many instances
      // exist in the field.
      @JsonAlias("delimiter")
      @JsonProperty("fieldDelimiter") String fieldDelimiter,
      @JsonProperty("quote") String quote,
      @JsonProperty("escape") String escape,
      @JsonProperty("comment") String comment,
      @JsonProperty("skipFirstLine") Boolean skipFirstLine,
      @JsonProperty("extractHeader") Boolean extractHeader) {
    this.extensions = extensions == null ?
        ImmutableList.of() : ImmutableList.copyOf(extensions);
    this.lineDelimiter = Strings.isNullOrEmpty(lineDelimiter) ? "\n" : lineDelimiter;
    this.fieldDelimiter = Strings.isNullOrEmpty(fieldDelimiter) ? ',' : fieldDelimiter.charAt(0);
    this.quote = Strings.isNullOrEmpty(quote) ? '"' : quote.charAt(0);
    this.escape = Strings.isNullOrEmpty(escape) ? '"' : escape.charAt(0);
    this.comment = Strings.isNullOrEmpty(comment) ? '#' : comment.charAt(0);
    this.skipFirstLine = skipFirstLine == null ? false : skipFirstLine;
    this.extractHeader = extractHeader == null ? false : extractHeader;
  }

  public List<String> getExtensions() { return extensions; }
  public String getLineDelimiter() { return lineDelimiter; }
  public char getFieldDelimiter() { return fieldDelimiter; }
  public char getQuote() { return quote; }
  public char getEscape() { return escape; }
  public char getComment() { return comment; }
  public boolean isSkipFirstLine() { return skipFirstLine; }
  @JsonProperty("extractHeader")
  public boolean isHeaderExtractionEnabled() { return extractHeader; }

  /**
   * Used for JSON serialization to handle \u0000 value which
   * Jackson converts to a null string.
   */
  @JsonProperty("fieldDelimiter")
  public String firldDeliterString() {
    return Character.toString(fieldDelimiter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(extensions, lineDelimiter, fieldDelimiter,
        quote, escape, comment, skipFirstLine, extractHeader);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    TextFormatConfig other = (TextFormatConfig) obj;
    return Objects.equals(extensions, other.extensions) &&
           Objects.equals(lineDelimiter, other.lineDelimiter) &&
           Objects.equals(fieldDelimiter, other.fieldDelimiter) &&
           Objects.equals(quote, other.quote) &&
           Objects.equals(escape, other.escape) &&
           Objects.equals(comment, other.comment) &&
           Objects.equals(skipFirstLine, other.skipFirstLine) &&
           Objects.equals(extractHeader, other.extractHeader);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("extensions", extensions)
      .field("skipFirstLine", skipFirstLine)
      .field("extractHeader", extractHeader)
      .escapedField("fieldDelimiter", fieldDelimiter)
      .escapedField("lineDelimiter", lineDelimiter)
      .escapedField("quote", quote)
      .escapedField("escape", escape)
      .escapedField("comment", comment)
      .toString();
  }
}
