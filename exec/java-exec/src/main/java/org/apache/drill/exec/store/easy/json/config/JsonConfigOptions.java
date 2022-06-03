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

package org.apache.drill.exec.store.easy.json.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderOptions;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonDeserialize(builder = JsonConfigOptions.JsonConfigOptionsBuilder.class)
public class JsonConfigOptions {

  @JsonProperty
  protected final Boolean allowNanInf;

  @JsonProperty
  protected final Boolean allTextMode;

  @JsonProperty
  protected final Boolean readNumbersAsDouble;

  @JsonProperty
  protected final Boolean enableEscapeAnyChar;

  @JsonProperty
  protected final Boolean skipMalformedRecords;

  @JsonProperty
  protected final Boolean skipMalformedDocument;

  public JsonConfigOptions(Boolean allowNanInf,
                           Boolean allTextMode,
                           Boolean readNumbersAsDouble,
                           Boolean enableEscapeAnyChar,
                           Boolean skipMalformedDocument,
                           Boolean skipMalformedRecords) {
    this.allowNanInf = allowNanInf;
    this.allTextMode = allTextMode;
    this.readNumbersAsDouble = readNumbersAsDouble;
    this.enableEscapeAnyChar = enableEscapeAnyChar;
    this.skipMalformedDocument = skipMalformedDocument;
    this.skipMalformedRecords = skipMalformedRecords;
  }

  JsonConfigOptions(JsonConfigOptionsBuilder builder) {
    this.allowNanInf = builder.allowNanInf;
    this.allTextMode = builder.allTextMode;
    this.readNumbersAsDouble = builder.readNumbersAsDouble;
    this.enableEscapeAnyChar = builder.enableEscapeAnyChar;
    this.skipMalformedRecords = builder.skipMalformedRecords;
    this.skipMalformedDocument = builder.skipMalformedDocument;
  }

  public static JsonConfigOptionsBuilder builder() {
    return new JsonConfigOptionsBuilder();
  }

  @JsonIgnore
  public JsonLoaderOptions getJsonOptions(OptionSet optionSet) {
    JsonLoaderOptions options = new JsonLoaderOptions(optionSet);
    if (allowNanInf != null) {
      options.allowNanInf = allowNanInf;
    }
    if (allTextMode != null) {
      options.allTextMode = allTextMode;
    }
    if (readNumbersAsDouble != null) {
      options.readNumbersAsDouble = readNumbersAsDouble;
    }
    if (enableEscapeAnyChar != null) {
      options.enableEscapeAnyChar = enableEscapeAnyChar;
    }
    if (skipMalformedRecords != null) {
      options.skipMalformedRecords = skipMalformedRecords;
    }
    if (skipMalformedDocument != null) {
      options.skipMalformedDocument = skipMalformedDocument;
    }

    return options;
  }

  @JsonProperty("allowNanInf")
  public Boolean allowNanInf() {
    return this.allowNanInf;
  }

  @JsonProperty("allTextMode")
  public Boolean allTextMode() {
    return this.allTextMode;
  }

  @JsonProperty("readNumbersAsDouble")
  public Boolean readNumbersAsDouble() {
    return this.readNumbersAsDouble;
  }

  @JsonProperty("enableEscapeAnyChar")
  public Boolean enableEscapeAnyChar() {
    return this.enableEscapeAnyChar;
  }

  @JsonProperty("skipMalformedRecords")
  public Boolean skipMalformedRecords() {
    return this.skipMalformedRecords;
  }

  @JsonProperty("skipMalformedDocument")
  public Boolean skipMalformedDocument() {
    return this.skipMalformedDocument;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JsonConfigOptions that = (JsonConfigOptions) o;
    return Objects.equals(allowNanInf, that.allowNanInf)
      && Objects.equals(allTextMode, that.allTextMode)
      && Objects.equals(readNumbersAsDouble, that.readNumbersAsDouble)
      && Objects.equals(enableEscapeAnyChar, that.enableEscapeAnyChar)
      && Objects.equals(skipMalformedDocument, that.skipMalformedDocument)
      && Objects.equals(skipMalformedRecords, that.skipMalformedRecords);
  }

  @Override
  public int hashCode() {
    return Objects.hash(allowNanInf, allTextMode, readNumbersAsDouble, enableEscapeAnyChar, skipMalformedDocument, skipMalformedRecords);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("allowNanInf", allowNanInf)
      .field("allTextMode", allTextMode)
      .field("readNumbersAsDouble", readNumbersAsDouble)
      .field("enableEscapeAnyChar", enableEscapeAnyChar)
      .field("skipMalformedRecords", skipMalformedRecords)
      .field("skipMalformedDocument", skipMalformedDocument)
      .toString();
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class JsonConfigOptionsBuilder {
    public Boolean allowNanInf;

    public Boolean allTextMode;

    public Boolean readNumbersAsDouble;

    public Boolean enableEscapeAnyChar;

    public Boolean skipMalformedRecords;

    public Boolean skipMalformedDocument;

    public JsonConfigOptionsBuilder allowNanInf(Boolean allowNanInf) {
      this.allowNanInf = allowNanInf;
      return this;
    }

    public JsonConfigOptionsBuilder allTextMode(Boolean allTextMode) {
      this.allTextMode = allTextMode;
      return this;
    }

    public JsonConfigOptionsBuilder readNumbersAsDouble(Boolean readNumbersAsDouble) {
      this.readNumbersAsDouble = readNumbersAsDouble;
      return this;
    }

    public JsonConfigOptionsBuilder enableEscapeAnyChar(Boolean enableEscapeAnyChar) {
      this.enableEscapeAnyChar = enableEscapeAnyChar;
      return this;
    }

    public JsonConfigOptionsBuilder skipMalformedRecords(Boolean skipMalformedRecords) {
      this.skipMalformedRecords = skipMalformedRecords;
      return this;
    }

    public JsonConfigOptionsBuilder skipMalformedDocument(Boolean skipMalformedDocument) {
      this.skipMalformedDocument = skipMalformedDocument;
      return this;
    }

    public JsonConfigOptions build() {
      return new JsonConfigOptions(this);
    }
  }
}

