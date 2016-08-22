/**
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
package org.apache.drill.exec.store.ischema;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

@JsonTypeName("info-schema")
public class InfoSchemaGroupScan extends AbstractGroupScan{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InfoSchemaGroupScan.class);

  private final InfoSchemaTableType table;
  private final InfoSchemaFilter filter;

  private boolean isFilterPushedDown = false;

  public InfoSchemaGroupScan(InfoSchemaTableType table) {
    this(table, null);
  }

  @JsonCreator
  public InfoSchemaGroupScan(@JsonProperty("table") InfoSchemaTableType table,
                             @JsonProperty("filter") InfoSchemaFilter filter) {
    super((String)null);
    this.table = table;
    this.filter = filter;
  }

  private InfoSchemaGroupScan(InfoSchemaGroupScan that) {
    super(that);
    this.table = that.table;
    this.filter = that.filter;
    this.isFilterPushedDown = that.isFilterPushedDown;
  }

  @JsonProperty("table")
  public InfoSchemaTableType getTable() {
    return table;
  }

  @JsonProperty("filter")
  public InfoSchemaFilter getFilter() {
    return filter;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
    Preconditions.checkArgument(endpoints.size() == 1);
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    Preconditions.checkArgument(minorFragmentId == 0);
    return new InfoSchemaSubScan(table, filter);
  }

  public ScanStats getScanStats(){
    if (filter == null) {
      return ScanStats.TRIVIAL_TABLE;
    } else {
      // If the filter is available, return stats that is lower in cost than TRIVIAL_TABLE cost so that
      // Scan with Filter is chosen.
      return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, 10, 1, 0);
    }
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 1;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    return new InfoSchemaGroupScan (this);
  }

  @Override
  public String getDigest() {
    return this.table.toString() + ", filter=" + filter;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    InfoSchemaGroupScan  newScan = new InfoSchemaGroupScan (this);
    return newScan;
  }

  public void setFilterPushedDown(boolean status) {
    this.isFilterPushedDown = status;
  }

  @JsonIgnore
  public boolean isFilterPushedDown() {
    return isFilterPushedDown;
  }
}
