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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.calcite.util.BitSets;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.base.FileGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillScanRel;
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
  private final DrillScanRel scanRel;

  public FileSystemPartitionDescriptor(PlannerSettings settings, DrillScanRel scanRel) {
    this.partitionLabel = settings.getFsPartitionColumnLabel();
    this.partitionLabelLength = partitionLabel.length();
    this.scanRel = scanRel;
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
        if (partitionLocation.getPartitionValue(partitionColumnIndex) == null) {
          throw new DrillRuntimeException("Value for directory cannot be null");
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
    final FormatSelection origSelection = (FormatSelection) scanRel.getDrillTable().getSelection();
    return origSelection.getSelection().selectionRoot;
  }

  @Override
  protected void createPartitionSublists() {
    List<String> fileLocations = ((FormatSelection) scanRel.getDrillTable().getSelection()).getAsFiles();
    List<PartitionLocation> locations = new LinkedList<>();
    for (String file: fileLocations) {
      locations.add(new DFSPartitionLocation(MAX_NESTED_SUBDIRS, getBaseTableLocation(), file));
    }
    locationSuperList = Lists.partition(locations, PartitionDescriptor.PARTITION_BATCH_SIZE);
    sublistsCreated = true;
  }

}
