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
package org.apache.drill.exec.store.iceberg.format;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.exec.store.iceberg.snapshot.Snapshot;
import org.apache.drill.exec.store.iceberg.snapshot.SnapshotFactory;

import java.util.Map;

@Getter
@EqualsAndHashCode
@JsonTypeName(IcebergFormatPluginConfig.NAME)
public class IcebergFormatPluginConfig implements FormatPluginConfig {

  public static final String NAME = "iceberg";

  private final Map<String, String> properties;

  private final Snapshot snapshot;

  private final Boolean caseSensitive;

  private final Boolean includeColumnStats;

  private final Boolean ignoreResiduals;

  private final Long snapshotId;

  private final Long snapshotAsOfTime;

  private final Long fromSnapshotId;

  private final Long toSnapshotId;

  @Builder
  @JsonCreator
  public IcebergFormatPluginConfig(
    @JsonProperty("properties") Map<String, String> properties,
    @JsonProperty("caseSensitive") Boolean caseSensitive,
    @JsonProperty("includeColumnStats") Boolean includeColumnStats,
    @JsonProperty("ignoreResiduals") Boolean ignoreResiduals,
    @JsonProperty("snapshotId") Long snapshotId,
    @JsonProperty("snapshotAsOfTime") Long snapshotAsOfTime,
    @JsonProperty("fromSnapshotId") Long fromSnapshotId,
    @JsonProperty("toSnapshotId") Long toSnapshotId) {
    this.properties = properties;
    this.caseSensitive = caseSensitive;
    this.includeColumnStats = includeColumnStats;
    this.ignoreResiduals = ignoreResiduals;
    this.snapshotId = snapshotId;
    this.snapshotAsOfTime = snapshotAsOfTime;
    this.fromSnapshotId = fromSnapshotId;
    this.toSnapshotId = toSnapshotId;

    SnapshotFactory.SnapshotContext context = SnapshotFactory.SnapshotContext.builder()
      .snapshotId(snapshotId)
      .snapshotAsOfTime(snapshotAsOfTime)
      .fromSnapshotId(fromSnapshotId)
      .toSnapshotId(toSnapshotId)
      .build();

    this.snapshot = SnapshotFactory.INSTANCE.createSnapshot(context);
  }

  @JsonIgnore
  public Snapshot getSnapshot() {
    return snapshot;
  }
}
