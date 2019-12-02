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
package org.apache.drill.exec.store.base;

import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.shaded.guava.com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Options for the "test mule" storage plugin. The options
 * control how the plugin behaves for testing. A real plugin
 * would simply implement project push down, and, if filter push-down
 * is needed, would implement one of the strategies described
 * here.
 */

@JsonTypeName(DummyStoragePluginConfig.NAME)
@JsonPropertyOrder({"enableProjectPushDown", "enableFilterPushDown",
  "keepFilters", "enable"})
public class DummyStoragePluginConfig extends StoragePluginConfig {

  public static final String NAME = "dummy";

  /**
   * Whether to enable or disable project push down.
   */
  private final boolean enableProjectPushDown;

  /**
   * Whether to enable or disable filter push down.
   */
  private final boolean enableFilterPushDown;

  /**
   * When doing filter push-down, whether to keep the filters in
   * the plan, or remove them (because they are done (simulated)
   * in the reader.
   */

  private final boolean keepFilters;

  public DummyStoragePluginConfig(
      @JsonProperty("enableProjectPushDown") boolean enableProjectPushDown,
      @JsonProperty("enableFilterPushDown") boolean enableFilterPushDown,
      @JsonProperty("keepFilters") boolean keepFilters) {
    this.enableProjectPushDown = enableProjectPushDown;
    this.enableFilterPushDown = enableFilterPushDown;
    this.keepFilters = keepFilters;
    setEnabled(true);
  }

  @JsonProperty("enableProjectPushDown")
  public boolean enableProjectPushDown() { return enableProjectPushDown; }

  @JsonProperty("enableFilterPushDown")
  public boolean enableFilterPushDown() { return enableFilterPushDown; }

  @JsonProperty("keepFilters")
  public boolean keepFilters() { return keepFilters; }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o == null || !(o instanceof DummyStoragePluginConfig)) {
      return false;
    }
    DummyStoragePluginConfig other = (DummyStoragePluginConfig) o;
    return enableProjectPushDown == other.enableProjectPushDown &&
           enableFilterPushDown == other.enableFilterPushDown &&
           keepFilters == other.keepFilters;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(enableProjectPushDown, enableFilterPushDown, keepFilters);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
        .field("enableProjectPushDown", enableProjectPushDown)
        .field("enableFilterPushDown", enableFilterPushDown)
        .field("keepFilters", keepFilters)
        .toString();
  }
}
