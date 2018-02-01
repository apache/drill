/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.base;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.proto.CoordinationProtos;

import java.util.List;

/**
 *  The type of scan operator, which allows to scan schemaless tables ({@link DynamicDrillTable} with null selection)
 */
@JsonTypeName("schemaless-scan")
public class SchemalessScan extends AbstractFileGroupScan implements SubScan {

  private final String selectionRoot;

  public SchemalessScan(String userName, String selectionRoot) {
    super(userName);
    this.selectionRoot = selectionRoot;
  }

  public SchemalessScan(final SchemalessScan that) {
    super(that);
    this.selectionRoot = that.selectionRoot;
  }

  @Override
  public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    return this;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 1;
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public String toString() {
    final String pattern = "SchemalessScan [selectionRoot = %s]";
    return String.format(pattern, selectionRoot);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    assert children == null || children.isEmpty();
    return new SchemalessScan(this);
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    return this;
  }

  @Override
  public ScanStats getScanStats() {
    return ScanStats.ZERO_RECORD_TABLE;
  }

  @Override
  public boolean supportsPartitionFilterPushdown() {
    return false;
  }

}
