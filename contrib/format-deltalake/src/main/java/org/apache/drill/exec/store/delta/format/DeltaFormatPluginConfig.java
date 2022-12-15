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
package org.apache.drill.exec.store.delta.format;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.logical.FormatPluginConfig;

import java.util.Objects;

@JsonTypeName(DeltaFormatPluginConfig.NAME)
public class DeltaFormatPluginConfig implements FormatPluginConfig {

  public static final String NAME = "delta";

  private final Long version;
  private final Long timestamp;

  @JsonCreator
  public DeltaFormatPluginConfig(@JsonProperty("version") Long version,
    @JsonProperty("timestamp") Long timestamp) {
    this.version = version;
    this.timestamp = timestamp;
  }

  @JsonProperty("version")
  public Long getVersion() {
    return version;
  }

  @JsonProperty("timestamp")
  public Long getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeltaFormatPluginConfig that = (DeltaFormatPluginConfig) o;
    return Objects.equals(version, that.version) && Objects.equals(timestamp, that.timestamp);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("version", version)
      .field("timestamp", timestamp)
      .toString();
  }
}
