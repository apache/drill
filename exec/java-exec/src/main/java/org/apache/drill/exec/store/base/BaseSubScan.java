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

import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractSubScan;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.store.StoragePluginRegistry;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Specification for a specific execution-time scan. Holds the information
 * passed form the Planner to the Drillbit so that the Drillbit can execute
 * the scan. Holds the config of the storage plugin associated with the
 * scan. The config allows creating a storage plugin instance, as needed,
 * to implement the scan.
 *
 * @see BaseGroupScan {@code BaseGroupScan} for additional details of the scan
 * life-cycle.
 */

@JsonTypeName("base-sub-scan")
public class BaseSubScan extends AbstractSubScan {

  protected final BaseStoragePlugin<?> storagePlugin;
  protected final List<SchemaPath> columns;

  public BaseSubScan(BaseGroupScan groupScan) {
    super(groupScan.getUserName());
    storagePlugin = groupScan.storagePlugin();
    this.columns = groupScan.getColumns();
  }

  @JsonCreator
  public BaseSubScan(
      @JsonProperty("userName") String userName,
      @JsonProperty("config") StoragePluginConfig config,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JacksonInject StoragePluginRegistry engineRegistry) {
    super(userName);
    this.storagePlugin = BaseStoragePlugin.resolvePlugin(engineRegistry, config);
    this.columns = columns;
  }

  @JsonProperty("config")
  public StoragePluginConfig getConfig() { return storagePlugin.getConfig(); }

  @JsonProperty("columns")
  public List<SchemaPath> columns() { return columns; }

  @SuppressWarnings("unchecked")
  public <T extends BaseStoragePlugin<?>> T storagePlugin() {
    return (T) storagePlugin;
  }

  @Override
  @JsonIgnore
  public int getOperatorType() {
    return storagePlugin.options().readerId;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public String toString() {
    PlanStringBuilder builder = new PlanStringBuilder(this);
    buildPlanString(builder);
    return builder.toString();
  }

  public void buildPlanString(PlanStringBuilder builder) {
    builder.field("user", getUserName());
    builder.field("columns", columns);
  }
}
