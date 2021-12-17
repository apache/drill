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

package org.apache.drill.exec.store.pdf;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import technology.tabula.extractors.BasicExtractionAlgorithm;
import technology.tabula.extractors.ExtractionAlgorithm;
import technology.tabula.extractors.SpreadsheetExtractionAlgorithm;

import java.util.Collections;
import java.util.List;


@Slf4j
@Builder
@Getter
@Setter
@Accessors(fluent = true)
@EqualsAndHashCode
@ToString
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonDeserialize(builder = PdfFormatConfig.PdfFormatConfigBuilder.class)
@JsonTypeName(PdfFormatPlugin.DEFAULT_NAME)
public class PdfFormatConfig implements FormatPluginConfig {

  @JsonProperty
  private final List<String> extensions;

  @JsonProperty
  private final boolean combinePages;

  @JsonProperty
  private final boolean extractHeaders;

  @JsonProperty
  private final String extractionAlgorithm;

  @JsonProperty
  private final String password;

  @JsonProperty
  private final int defaultTableIndex;

  private PdfFormatConfig(PdfFormatConfig.PdfFormatConfigBuilder builder) {
    this.extensions = builder.extensions == null ? Collections.singletonList("pdf") : ImmutableList.copyOf(builder.extensions);
    this.combinePages = builder.combinePages;
    this.extractHeaders = builder.extractHeaders;
    this.defaultTableIndex = builder.defaultTableIndex;
    this.extractionAlgorithm = builder.extractionAlgorithm;
    this.password = builder.password;
  }

  @JsonIgnore
  public PdfBatchReader.PdfReaderConfig getReaderConfig(PdfFormatPlugin plugin) {
    return new PdfBatchReader.PdfReaderConfig(plugin);
  }

  @JsonIgnore
  public ExtractionAlgorithm getAlgorithm() {
    if (StringUtils.isEmpty(this.extractionAlgorithm) || this.extractionAlgorithm.equalsIgnoreCase("basic")) {
      return new BasicExtractionAlgorithm();
    } else if (this.extractionAlgorithm.equalsIgnoreCase("spreadsheet")) {
      return new SpreadsheetExtractionAlgorithm();
    } else {
      throw UserException.validationError()
        .message(extractionAlgorithm + " is not a valid extraction algorithm. The available choices are basic or spreadsheet.")
        .build(logger);
    }
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class PdfFormatConfigBuilder {
    public PdfFormatConfig build() {
      return new PdfFormatConfig(this);
    }
  }
}
