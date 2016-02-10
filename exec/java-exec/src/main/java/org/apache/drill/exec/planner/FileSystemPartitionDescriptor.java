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

import java.io.IOException;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.util.BitSets;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.base.FileGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DrillTranslatableTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;


// partition descriptor for file system based tables
public class FileSystemPartitionDescriptor extends AbstractPartitionDescriptor {

  static final int MAX_NESTED_SUBDIRS = 10;          // allow up to 10 nested sub-directories

  private final String partitionLabel;
  private final int partitionLabelLength;
  private final Map<String, Integer> partitions = Maps.newHashMap();
  private final TableScan scanRel;
  private final DrillTable table;

  public FileSystemPartitionDescriptor(PlannerSettings settings, TableScan scanRel) {
    Preconditions.checkArgument(scanRel instanceof DrillScanRel || scanRel instanceof EnumerableTableScan);
    this.partitionLabel = settings.getFsPartitionColumnLabel();
    this.partitionLabelLength = partitionLabel.length();
    this.scanRel = scanRel;
    DrillTable unwrap;
    unwrap = scanRel.getTable().unwrap(DrillTable.class);
    if (unwrap == null) {
      unwrap = scanRel.getTable().unwrap(DrillTranslatableTable.class).getDrillTable();
    }

    table = unwrap;

    for(int i =0; i < 10; i++){
      partitions.put(partitionLabel + i, i);
    }
  }

  @Override
  public int getPartitionHierarchyIndex(String partitionName) {
    String suffix = partitionName.substring(partitionLabelLength); // get the numeric suffix from 'dir<N>'
    return Integer.parseInt(suffix);
  }

  @Override
  public boolean isPartitionName(String name) {
    return partitions.containsKey(name);
  }

  @Override
  public Integer getIdIfValid(String name) {
    return partitions.get(name);
  }

  @Override
  public int getMaxHierarchyLevel() {
    return MAX_NESTED_SUBDIRS;
  }

//  @Override
//  public GroupScan createNewGroupScan(List<String> newFiles) throws IOException {
//    if (scanRel instanceof DrillScanRel) {
//      final FileSelection newFileSelection = new FileSelection(null, newFiles, getBaseTableLocation());
//      final FileGroupScan newScan = ((FileGroupScan)((DrillScanRel)scanRel).getGroupScan()).clone(newFileSelection);
//      return newScan;
//    } else {
//      throw new UnsupportedOperationException("Does not allow to get groupScan for EnumerableTableScan");
//    }
//  }

  public DrillTable getTable() {
    return table;
  }

  @Override
  public void populatePartitionVectors(ValueVector[] vectors, List<PartitionLocation> partitions,
                                       BitSet partitionColumnBitSet, Map<Integer, String> fieldNameMap) {
    int record = 0;
    for (PartitionLocation partitionLocation: partitions) {
      for (int partitionColumnIndex : BitSets.toIter(partitionColumnBitSet)) {
        if (partitionLocation.getPartitionValue(partitionColumnIndex) == null) {
          // set null if dirX does not exist for the location.
          ((NullableVarCharVector) vectors[partitionColumnIndex]).getMutator().setNull(record);
        } else {
          byte[] bytes = (partitionLocation.getPartitionValue(partitionColumnIndex)).getBytes(Charsets.UTF_8);
          ((NullableVarCharVector) vectors[partitionColumnIndex]).getMutator().setSafe(record, bytes, 0, bytes.length);
        }
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
    return Types.optional(TypeProtos.MinorType.VARCHAR);
  }

  public String getName(int index) {
    return partitionLabel + index;
  }

  private String getBaseTableLocation() {
    final FormatSelection origSelection = (FormatSelection) table.getSelection();
    return origSelection.getSelection().selectionRoot;
  }

  @Override
  protected void createPartitionSublists() {
    List<String> fileLocations = ((FormatSelection) table.getSelection()).getAsFiles();
    List<PartitionLocation> locations = new LinkedList<>();
    for (String file: fileLocations) {
      locations.add(new DFSPartitionLocation(MAX_NESTED_SUBDIRS, getBaseTableLocation(), file));
    }
    locationSuperList = Lists.partition(locations, PartitionDescriptor.PARTITION_BATCH_SIZE);
    sublistsCreated = true;
  }

  @Override
  public TableScan createTableScan(List<String> newFiles) throws Exception {
    if (scanRel instanceof DrillScanRel) {
      final FileSelection newFileSelection = new FileSelection(null, newFiles, getBaseTableLocation());
      final FileGroupScan newGroupScan = ((FileGroupScan)((DrillScanRel)scanRel).getGroupScan()).clone(newFileSelection);
      return new DrillScanRel(scanRel.getCluster(),
                      scanRel.getTraitSet().plus(DrillRel.DRILL_LOGICAL),
                      scanRel.getTable(),
                      newGroupScan,
                      scanRel.getRowType(),
                      ((DrillScanRel) scanRel).getColumns(),
                      true /*filter pushdown*/);
    } else if (scanRel instanceof EnumerableTableScan) {
      return createNewTableScanFromSelection((EnumerableTableScan)scanRel, newFiles);
    } else {
      throw new UnsupportedOperationException("Only DrillScanRel and EnumerableTableScan is allowed!");
    }
  }

  private TableScan createNewTableScanFromSelection(EnumerableTableScan oldScan, List<String> newFiles) {
    final RelOptTableImpl t = (RelOptTableImpl) oldScan.getTable();
    final FormatSelection formatSelection = (FormatSelection) table.getSelection();
    final FileSelection newFileSelection = new FileSelection(null, newFiles, getBaseTableLocation());
    final FormatSelection newFormatSelection = new FormatSelection(formatSelection.getFormat(), newFileSelection);
    final DrillTranslatableTable newTable = new DrillTranslatableTable(
            new DynamicDrillTable(table.getPlugin(), table.getStorageEngineName(),
            table.getUserName(),
            newFormatSelection));
    final RelOptTableImpl newOptTableImpl = RelOptTableImpl.create(t.getRelOptSchema(), t.getRowType(), newTable);

    return EnumerableTableScan.create(oldScan.getCluster(), newOptTableImpl);
  }

}
