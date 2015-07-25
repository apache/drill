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
package org.apache.drill.exec.planner.sql;

import org.apache.calcite.util.BitSets;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.fn.impl.DateUtility;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.PartitionDescriptor;
import org.apache.drill.exec.planner.PartitionLocation;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.store.hive.HiveDataTypeUtility;
import org.apache.drill.exec.store.hive.HiveReadEntry;
import org.apache.drill.exec.store.hive.HiveScan;
import org.apache.drill.exec.store.hive.HiveTable;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableDateVector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableTimeStampVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import java.sql.Timestamp;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

// Partition descriptor for hive tables
public class HivePartitionDescriptor implements PartitionDescriptor {

  private final Map<String, Integer> partitionMap = new HashMap<>();
  private final int MAX_NESTED_SUBDIRS;
  private final DrillScanRel scanRel;

  public HivePartitionDescriptor(PlannerSettings settings, DrillScanRel scanRel) {
    int i = 0;
    this.scanRel = scanRel;
    for (HiveTable.FieldSchemaWrapper wrapper : ((HiveScan) scanRel.getGroupScan()).hiveReadEntry.table.partitionKeys) {
      partitionMap.put(wrapper.name, i);
      i++;
    }
    MAX_NESTED_SUBDIRS = i;
  }

  @Override
  public int getPartitionHierarchyIndex(String partitionName) {
    return partitionMap.get(partitionName);
  }

  @Override
  public boolean isPartitionName(String name) {
    return (partitionMap.get(name) != null);
  }

  @Override
  public int getMaxHierarchyLevel() {
    return MAX_NESTED_SUBDIRS;
  }

  public String getBaseTableLocation() {
    HiveReadEntry origEntry = ((HiveScan) scanRel.getGroupScan()).hiveReadEntry;
    return origEntry.table.getTable().getSd().getLocation();
  }

  @Override
  public GroupScan createNewGroupScan(List<String> newFiles) throws ExecutionSetupException {
    HiveScan hiveScan = (HiveScan) scanRel.getGroupScan();
    HiveReadEntry origReadEntry = hiveScan.hiveReadEntry;
    List<HiveTable.HivePartition> oldPartitions = origReadEntry.partitions;
    List<HiveTable.HivePartition> newPartitions = new LinkedList<>();

    for (HiveTable.HivePartition part: oldPartitions) {
      String partitionLocation = part.getPartition().getSd().getLocation();
      for (String newPartitionLocation: newFiles) {
        if (partitionLocation.equals(newPartitionLocation)) {
          newPartitions.add(part);
        }
      }
    }

    HiveReadEntry newReadEntry = new HiveReadEntry(origReadEntry.table, newPartitions, origReadEntry.hiveConfigOverride);
    HiveScan newScan = new HiveScan(hiveScan.getUserName(), newReadEntry, hiveScan.storagePlugin, hiveScan.columns);
    return newScan;
  }

  @Override
  public List<PartitionLocation> getPartitions() {
    List<PartitionLocation> partitions = new LinkedList<>();
    HiveReadEntry origEntry = ((HiveScan) scanRel.getGroupScan()).hiveReadEntry;
    List<String> allFileLocations = new LinkedList<>();
    for (Partition partition: origEntry.getPartitions()) {
      allFileLocations.add(partition.getSd().getLocation());
    }
    for (String file: allFileLocations) {
      partitions.add(new HivePartitionLocation(MAX_NESTED_SUBDIRS, getBaseTableLocation(),file));
    }
    return partitions;
  }

  @Override
  public void populatePartitionVectors(ValueVector[] vectors, List<PartitionLocation> partitions,
                                       BitSet partitionColumnBitSet, Map<Integer, String> fieldNameMap) {
    int record = 0;
    for(PartitionLocation partitionLocation: partitions) {
      for(int partitionColumnIndex : BitSets.toIter(partitionColumnBitSet)){
        populateVector(vectors[partitionColumnIndex], partitionLocation.getPartitionValue(partitionColumnIndex),
            record);
      }
      record++;
    }

    for(ValueVector v : vectors) {
      if(v == null){
        continue;
      }
      v.getMutator().setValueCount(partitions.size());
    }
  }

  @Override
  public TypeProtos.MajorType getVectorType(SchemaPath column, PlannerSettings plannerSettings) {
    HiveScan hiveScan = (HiveScan) scanRel.getGroupScan();
    String partitionName = column.getAsNamePart().getName();
    Map<String, String> partitionNameTypeMap = hiveScan.hiveReadEntry.table.getPartitionNameTypeMap();
    String hiveType = partitionNameTypeMap.get(partitionName);
    PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(hiveType);

    TypeProtos.MinorType partitionType = HiveDataTypeUtility.getMinorTypeFromHivePrimitiveTypeInfo(primitiveTypeInfo,
        plannerSettings.getOptions());
    return TypeProtos.MajorType.newBuilder().setMode(TypeProtos.DataMode.OPTIONAL).setMinorType(partitionType).build();
  }

  @Override
  public Integer getIdIfValid(String name) {
    return partitionMap.get(name);
  }

  public static void populateVector(ValueVector vector, String value, int record) {
    TypeProtos.MinorType type = vector.getField().getType().getMinorType();

    switch (type) {
      case TINYINT:
      case SMALLINT:
      case INT:
        if (value == null) {
          ((NullableIntVector) vector).getMutator().setNull(record);
        } else {
          ((NullableIntVector) vector).getMutator().setSafe(record, Integer.parseInt(value));
        }
        break;

      case BIGINT:
        if (value == null) {
          ((NullableBigIntVector) vector).getMutator().setNull(record);
        } else {
          ((NullableBigIntVector) vector).getMutator().setSafe(record, Long.parseLong(value));
        }
        break;
      case FLOAT4:
        if (value == null) {
          ((NullableFloat4Vector) vector).getMutator().setNull(record);
        } else {
          ((NullableFloat4Vector) vector).getMutator().setSafe(record, Float.parseFloat(value));
        }
        break;
      case FLOAT8:
        if (value == null) {
          ((NullableFloat8Vector) vector).getMutator().setNull(record);
        } else {
          ((NullableFloat8Vector) vector).getMutator().setSafe(record, Double.parseDouble(value));
        }
        break;
      case TIMESTAMP:
        if (value == null) {
          ((NullableTimeStampVector) vector).getMutator().setNull(record);
        } else {
          DateTimeFormatter f = DateUtility.getDateTimeFormatter();
          value = value.replace("%3A", ":");
          long ts = DateTime.parse(value, f).withZoneRetainFields(DateTimeZone.UTC).getMillis();
          ((NullableTimeStampVector) vector).getMutator().set(record, ts);
        }
        break;
      case DATE:
        if (value == null) {
          ((NullableDateVector) vector).getMutator().setNull(record);
        } else {
          DateTimeFormatter f = DateUtility.formatDate;
          long ts = DateTime.parse(value, f).withZoneRetainFields(DateTimeZone.UTC).getMillis();
          ((NullableDateVector) vector).getMutator().set(record, ts);
        }
        break;
      case VARCHAR:
        if (value == null) {
          ((NullableVarCharVector) vector).getMutator().setNull(record);
        } else {
          ((NullableVarCharVector) vector).getMutator().set(record, value.getBytes());
        }
        break;
    }
  }
}
