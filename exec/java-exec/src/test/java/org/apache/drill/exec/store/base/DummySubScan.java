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
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.base.filter.RelOp;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Dummy sub scan that simply passes along the group scan
 * information. A real scan operator definition would likely translate the
 * group scan information into the form needed by the
 * underlying storage system.
 * <p>
 * For testing, we use a list of filters as a proxy for scans
 * of the underlying system. Each filter is a list of filter
 * conditions to be applied. (The dummy scan does not actually
 * do anything with these, other than holding them for use in
 * verifying plans.)
 */

@JsonTypeName("dummy-sub-scan")
@JsonPropertyOrder({"userName", "scanSpec", "columns",
                    "andFilters", "orFilters", "config"})
@JsonInclude(Include.NON_NULL)
public class DummySubScan extends BaseSubScan {

  private final DummyScanSpec scanSpec;
  private final List<List<RelOp>> filters;

  @JsonCreator
  public DummySubScan(
      @JsonProperty("userName") String userName,
      @JsonProperty("config") StoragePluginConfig config,
      @JsonProperty("scanSpec") DummyScanSpec scanSpec,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("filters") List<List<RelOp>> filters,
      @JacksonInject StoragePluginRegistry engineRegistry) {
    super(userName, config, columns, engineRegistry);
    this.scanSpec = scanSpec;
    this.filters = filters;
 }

  public DummySubScan(DummyGroupScan groupScan, List<List<RelOp>> filters) {
    super(groupScan);
    this.scanSpec = groupScan.scanSpec();
    this.filters = filters;
  }

  @JsonProperty("scanSpec")
  public DummyScanSpec scanSpec() { return scanSpec; }

  @JsonProperty("filters")
  public List<List<RelOp>> filters() { return filters; }

  @Override
  public void buildPlanString(PlanStringBuilder builder) {
    super.buildPlanString(builder);
    builder.field("scanSpec", scanSpec);
    builder.field("filters", filters);
  }
}
