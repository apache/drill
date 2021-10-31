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
package org.apache.drill.exec.store.plan.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.store.plan.PluginImplementor;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Storage plugin table scan rel implementation.
 */
public class StoragePluginTableScan extends DrillScanRelBase implements PluginRel {

  private final RelDataType rowType;

  public StoragePluginTableScan(RelOptCluster cluster, RelTraitSet traits, GroupScan grpScan,
      RelOptTable table, RelDataType rowType) {
    super(cluster, traits, grpScan.clone(getColumns(rowType)), table);
    this.rowType = rowType;
  }

  @Override
  public void implement(PluginImplementor implementor) throws IOException {
    implementor.implement(this);
  }

  @Override
  public DrillScanRelBase copy(RelTraitSet traitSet, GroupScan scan, RelDataType rowType) {
    return new StoragePluginTableScan(getCluster(), traitSet, scan, getTable(), rowType);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return getGroupScan().getScanStats(mq).getRecordCount();
  }

  @Override
  public RelDataType deriveRowType() {
    return this.rowType;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("groupScan", getGroupScan().getDigest());
  }

  @Override
  protected String computeDigest() {
    return super.computeDigest();
  }

  @Override
  public boolean canImplement(PluginImplementor implementor) {
    return implementor.canImplement(this);
  }

  private static List<SchemaPath> getColumns(RelDataType rowType) {
    return rowType.getFieldList().stream()
      .map(filed -> filed.isDynamicStar()
        ? SchemaPath.STAR_COLUMN
        : SchemaPath.getSimplePath(filed.getName()))
      .collect(Collectors.toList());
  }
}
