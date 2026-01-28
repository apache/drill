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
package org.apache.drill.exec.store.paimon.format;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.logical.FormatPluginConfig;

import java.util.Map;
import java.util.Objects;

@JsonTypeName(PaimonFormatPluginConfig.NAME)
@JsonDeserialize(builder = PaimonFormatPluginConfig.PaimonFormatPluginConfigBuilder.class)
public class PaimonFormatPluginConfig implements FormatPluginConfig {

  public static final String NAME = "paimon";

  private final Map<String, String> properties;

  // Time travel: load a specific snapshot id.
  private final Long snapshotId;

  // Time travel: load the latest snapshot at or before the given timestamp (millis).
  private final Long snapshotAsOfTime;

  @JsonCreator
  public PaimonFormatPluginConfig(PaimonFormatPluginConfigBuilder builder) {
    this.properties = builder.properties;
    this.snapshotId = builder.snapshotId;
    this.snapshotAsOfTime = builder.snapshotAsOfTime;
  }

  public static PaimonFormatPluginConfigBuilder builder() {
    return new PaimonFormatPluginConfigBuilder();
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public Long getSnapshotId() {
    return snapshotId;
  }

  public Long getSnapshotAsOfTime() {
    return snapshotAsOfTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PaimonFormatPluginConfig that = (PaimonFormatPluginConfig) o;
    return Objects.equals(properties, that.properties)
      && Objects.equals(snapshotId, that.snapshotId)
      && Objects.equals(snapshotAsOfTime, that.snapshotAsOfTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, snapshotId, snapshotAsOfTime);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("properties", properties)
      .field("snapshotId", snapshotId)
      .field("snapshotAsOfTime", snapshotAsOfTime)
      .toString();
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class PaimonFormatPluginConfigBuilder {
    private Map<String, String> properties;

    private Long snapshotId;

    private Long snapshotAsOfTime;

    public PaimonFormatPluginConfigBuilder properties(Map<String, String> properties) {
      this.properties = properties;
      return this;
    }

    public PaimonFormatPluginConfigBuilder snapshotId(Long snapshotId) {
      this.snapshotId = snapshotId;
      return this;
    }

    public PaimonFormatPluginConfigBuilder snapshotAsOfTime(Long snapshotAsOfTime) {
      this.snapshotAsOfTime = snapshotAsOfTime;
      return this;
    }

    public PaimonFormatPluginConfig build() {
      return new PaimonFormatPluginConfig(this);
    }
  }

}
