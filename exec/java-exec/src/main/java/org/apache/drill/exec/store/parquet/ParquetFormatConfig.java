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
package org.apache.drill.exec.store.parquet;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.drill.common.logical.FormatPluginConfig;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("parquet") @JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class ParquetFormatConfig implements FormatPluginConfig {

  public boolean autoCorrectCorruptDates = true;
  public boolean enableStringsSignedMinMax = false;

  /**
   * @return true if auto correction of corrupt dates is enabled, false otherwise
   */
  @JsonIgnore
  public boolean areCorruptDatesAutoCorrected() {
    return autoCorrectCorruptDates;
  }

  /**
   * Parquet statistics for UTF-8 data for files created prior to 1.9.1 parquet library version was stored incorrectly.
   * If user exactly knows that data in binary columns is in ASCII (not UTF-8), turning this property to 'true'
   * enables statistics usage for varchar and decimal columns.
   *
   * Can be overridden for individual tables using
   * @link org.apache.drill.exec.ExecConstants#PARQUET_READER_STRINGS_SIGNED_MIN_MAX} session option.
   *
   * @return true if string signed min max enabled, false otherwise
   */
  @JsonIgnore
  public boolean isStringsSignedMinMaxEnabled() {
    return enableStringsSignedMinMax;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ParquetFormatConfig that = (ParquetFormatConfig) o;

    if (autoCorrectCorruptDates != that.autoCorrectCorruptDates) {
      return false;
    }

    return enableStringsSignedMinMax == that.enableStringsSignedMinMax;
  }

  @Override
  public int hashCode() {
    int result = (autoCorrectCorruptDates ? 1231 : 1237);
    result = 31 * result + (enableStringsSignedMinMax ? 1231 : 1237);
    return result;
  }

  @Override
  public String toString() {
    return "ParquetFormatConfig{"
      + "autoCorrectCorruptDates=" + autoCorrectCorruptDates
      + ", enableStringsSignedMinMax=" + enableStringsSignedMinMax
      + '}';
  }
}
