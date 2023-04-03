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
package org.apache.drill.exec.store.easy.json;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.logical.FormatPluginConfig;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static org.apache.drill.exec.store.easy.json.JSONFormatPlugin.PLUGIN_NAME;

@JsonTypeName(PLUGIN_NAME)
public class JSONFormatConfig implements FormatPluginConfig {
  private static final List<String> DEFAULT_EXTS = ImmutableList.of("json");

  private final List<String> extensions;
  private final Boolean allTextMode;
  private final Boolean readNumbersAsDouble;
  private final Boolean skipMalformedJSONRecords;
  private final Boolean escapeAnyChar;
  private final Boolean nanInf;

  @JsonCreator
  public JSONFormatConfig(
      @JsonProperty("extensions") List<String> extensions,
      @JsonProperty("allTextMode") Boolean allTextMode,
      @JsonProperty("readNumbersAsDouble") Boolean readNumbersAsDouble,
      @JsonProperty("skipMalformedJSONRecords") Boolean skipMalformedJSONRecords,
      @JsonProperty("escapeAnyChar") Boolean escapeAnyChar,
      @JsonProperty("nanInf") Boolean nanInf) {
    this.extensions = extensions == null ? DEFAULT_EXTS : ImmutableList.copyOf(extensions);
    this.allTextMode = allTextMode;
    this.readNumbersAsDouble = readNumbersAsDouble;
    this.skipMalformedJSONRecords = skipMalformedJSONRecords;
    this.escapeAnyChar = escapeAnyChar;
    this.nanInf = nanInf;
  }

  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public List<String> getExtensions() {
    return extensions;
  }

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  public Boolean getAllTextMode() {
    return allTextMode;
  }

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  public Boolean getReadNumbersAsDouble() {
    return readNumbersAsDouble;
  }

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  public Boolean getSkipMalformedJSONRecords() {
    return skipMalformedJSONRecords;
  }

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  public Boolean getEscapeAnyChar() {
    return escapeAnyChar;
  }

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  public Boolean getNanInf() {
    return nanInf;
  }

  @Override
  public int hashCode() {
    return Objects.hash(extensions, allTextMode, readNumbersAsDouble, skipMalformedJSONRecords, escapeAnyChar, nanInf);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    JSONFormatConfig other = (JSONFormatConfig) obj;
    return Objects.deepEquals(extensions, other.extensions) &&
      Objects.equals(allTextMode, other.allTextMode) &&
      Objects.equals(readNumbersAsDouble, other.readNumbersAsDouble) &&
      Objects.equals(skipMalformedJSONRecords, other.skipMalformedJSONRecords) &&
      Objects.equals(escapeAnyChar, other.escapeAnyChar) &&
      Objects.equals(nanInf, other.nanInf);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("extensions", extensions)
      .field("allTextMode", allTextMode)
      .field("readNumbersAsDouble", readNumbersAsDouble)
      .field("skipMalformedRecords", skipMalformedJSONRecords)
      .field("escapeAnyChar", escapeAnyChar)
      .field("nanInf", nanInf)
      .toString();
  }
}
