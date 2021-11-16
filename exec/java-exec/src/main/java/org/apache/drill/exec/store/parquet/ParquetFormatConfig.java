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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.logical.FormatPluginConfig;

import com.fasterxml.jackson.annotation.JsonTypeName;

@EqualsAndHashCode
@JsonTypeName("parquet") @JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class ParquetFormatConfig implements FormatPluginConfig {

  @Getter private final boolean autoCorrectCorruptDates;

  /**
   * Parquet statistics for UTF-8 data in files created prior to 1.9.1 parquet
   * library version were stored incorrectly.  If the user exactly knows that
   * data in binary columns is in ASCII (not UTF-8), turning this property to
   * 'true' enables statistics usage for varchar and decimal columns.
   *
   * {@link org.apache.drill.exec.ExecConstants#PARQUET_READER_STRINGS_SIGNED_MIN_MAX}
   */
  @Getter private final boolean enableStringsSignedMinMax;

  // {@link org.apache.drill.exec.ExecConstants#PARQUET_BLOCK_SIZE}
  @Getter private final Integer blockSize;

  // {@link org.apache.drill.exec.ExecConstants#PARQUET_PAGE_SIZE}
  @Getter private final Integer pageSize;

  // {@link org.apache.drill.exec.ExecConstants#PARQUET_WRITER_USE_SINGLE_FS_BLOCK}
  @Getter private final Boolean useSingleFSBlock;

  // {@link org.apache.drill.exec.ExecConstants#PARQUET_WRITER_COMPRESSION_TYPE}
  @Getter private final String writerCompressionType;

  // {@link org.apache.drill.exec.ExecConstants#PARQUET_WRITER_LOGICAL_TYPE_FOR_DECIMALS}
  @Getter private final String writerLogicalTypeForDecimals;

  // {@link org.apache.drill.exec.ExecConstants#PARQUET_WRITER_USE_PRIMITIVE_TYPES_FOR_DECIMALS}
  @Getter private final Boolean writerUsePrimitivesForDecimals;

  // {@link org.apache.drill.exec.ExecConstants#PARQUET_WRITER_FORMAT_VERSION}
  @Getter private final String writerFormatVersion;

  public ParquetFormatConfig() {
    this(true, false, null, null, null, null, null, null, null);
  }

  @JsonCreator
  @Builder
  public ParquetFormatConfig(
    @JsonProperty("autoCorrectCorruptDates") Boolean autoCorrectCorruptDates,
    @JsonProperty("enableStringsSignedMinMax") boolean enableStringsSignedMinMax,
    @JsonProperty("blockSize") Integer blockSize,
    @JsonProperty("pageSize") Integer pageSize,
    @JsonProperty("useSingleFSBlock") Boolean useSingleFSBlock,
    @JsonProperty("writerCompressionType") String writerCompressionType,
    @JsonProperty("writerLogicalTypeForDecimals") String writerLogicalTypeForDecimals,
    @JsonProperty("writerUsePrimitivesForDecimals") Boolean writerUsePrimitivesForDecimals,
    @JsonProperty("writerFormatVersion") String writerFormatVersion
  ) {
    this.autoCorrectCorruptDates = autoCorrectCorruptDates == null ? true : autoCorrectCorruptDates;
    this.enableStringsSignedMinMax = enableStringsSignedMinMax;
    this.blockSize = blockSize;
    this.pageSize = pageSize;
    this.useSingleFSBlock = useSingleFSBlock;
    this.writerCompressionType = writerCompressionType;
    this.writerLogicalTypeForDecimals = writerLogicalTypeForDecimals;
    this.writerUsePrimitivesForDecimals = writerUsePrimitivesForDecimals;
    this.writerFormatVersion = writerFormatVersion;
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("autoCorrectCorruptDates", autoCorrectCorruptDates)
      .field("enableStringsSignedMinMax", enableStringsSignedMinMax)
      .field("blockSize", blockSize)
      .field("pageSize", pageSize)
      .field("useSingleFSBlock", useSingleFSBlock)
      .field("writerCompressionType", writerCompressionType)
      .field("writerLogicalTypeForDecimals", writerLogicalTypeForDecimals)
      .field("writerUsePrimitivesForDecimals", writerUsePrimitivesForDecimals)
      .field("writerFormatVersion", writerFormatVersion)
      .toString();
  }
}
