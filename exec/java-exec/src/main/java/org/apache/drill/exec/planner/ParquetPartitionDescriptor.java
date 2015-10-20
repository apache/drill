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
package org.apache.drill.exec.planner;

import org.apache.calcite.util.BitSets;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.base.FileGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.store.parquet.ParquetGroupScan;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * PartitionDescriptor that describes partitions based on column names instead of directory structure
 */
public class ParquetPartitionDescriptor extends AbstractPartitionDescriptor {

  private final List<SchemaPath> partitionColumns;
  private final DrillScanRel scanRel;
  static final int MAX_NESTED_SUBDIRS = 10;

  public ParquetPartitionDescriptor(PlannerSettings settings, DrillScanRel scanRel) {
    ParquetGroupScan scan = (ParquetGroupScan) scanRel.getGroupScan();
    this.partitionColumns = scan.getPartitionColumns();
    this.scanRel = scanRel;
  }

  @Override
  public int getPartitionHierarchyIndex(String partitionName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isPartitionName(String name) {
    return partitionColumns.contains(name);
  }

  @Override
  public Integer getIdIfValid(String name) {
    SchemaPath schemaPath = SchemaPath.getSimplePath(name);
    int id = partitionColumns.indexOf(schemaPath);
    if (id == -1) {
      return null;
    }
    return id;
  }

  @Override
  public int getMaxHierarchyLevel() {
    return partitionColumns.size();
  }

  @Override
  public GroupScan createNewGroupScan(List<String> newFiles) throws IOException {
    final FileSelection newFileSelection = new FileSelection(newFiles, getBaseTableLocation(), true);
    final FileGroupScan newScan = ((FileGroupScan)scanRel.getGroupScan()).clone(newFileSelection);
    return newScan;
  }

  @Override
  public void populatePartitionVectors(ValueVector[] vectors, List<PartitionLocation> partitions,
                                       BitSet partitionColumnBitSet, Map<Integer, String> fieldNameMap) {
    int record = 0;
    for (PartitionLocation partitionLocation: partitions) {
      for (int partitionColumnIndex : BitSets.toIter(partitionColumnBitSet)) {
        SchemaPath column = SchemaPath.getSimplePath(fieldNameMap.get(partitionColumnIndex));
        ((ParquetGroupScan) scanRel.getGroupScan()).populatePruningVector(vectors[partitionColumnIndex], record, column,
            partitionLocation.getEntirePartitionLocation());
      }
      record++;
    }

    for (ValueVector v : vectors) {
      if (v == null) {
        continue;
      }
      v.getMutator().setValueCount(partitions.size());
    }

  }

  @Override
  public TypeProtos.MajorType getVectorType(SchemaPath column, PlannerSettings plannerSettings) {
    return ((ParquetGroupScan) scanRel.getGroupScan()).getTypeForColumn(column);
  }

  private String getBaseTableLocation() {
    final FormatSelection origSelection = (FormatSelection) scanRel.getDrillTable().getSelection();
    return origSelection.getSelection().selectionRoot;
  }

  @Override
  protected void createPartitionSublists() {
    Set<String> fileLocations = ((ParquetGroupScan) scanRel.getGroupScan()).getFileSet();
    List<PartitionLocation> locations = new LinkedList<>();
    for (String file: fileLocations) {
      locations.add(new ParquetPartitionLocation(file));
    }
    locationSuperList = Lists.partition(locations, PartitionDescriptor.PARTITION_BATCH_SIZE);
    sublistsCreated = true;
  }

}
