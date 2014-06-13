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

import java.util.Collections;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.PhysicalOperatorSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

@JsonTypeName("info-schema")
public class InfoSchemaGroupScan extends AbstractGroupScan{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InfoSchemaGroupScan.class);

  private final SelectedTable table;

  private List<SchemaPath> columns;

  @JsonCreator
  public InfoSchemaGroupScan(@JsonProperty("table") SelectedTable table,
      @JsonProperty("columns") List<SchemaPath> columns) {
    this.table = table;
    this.columns = columns;
  }

  private InfoSchemaGroupScan(InfoSchemaGroupScan that) {
    this.table = that.table;
    this.columns = that.columns;
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
    Preconditions.checkArgument(endpoints.size() == 1);
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    Preconditions.checkArgument(minorFragmentId == 0);
    return new InfoSchemaSubScan(table);
  }

  public ScanStats getScanStats(){
    return ScanStats.TRIVIAL_TABLE;
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
  public List<EndpointAffinity> getOperatorAffinity() {
    return Collections.emptyList();
  }

  @Override
  public String getDigest() {
    return this.table.toString() + "columns=" + columns;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    InfoSchemaGroupScan  newScan = new InfoSchemaGroupScan (this);
    newScan.columns = columns;
    return newScan;
  }

}
