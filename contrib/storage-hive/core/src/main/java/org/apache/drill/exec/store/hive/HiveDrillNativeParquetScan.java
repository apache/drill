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
package org.apache.drill.exec.store.hive;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.hive.HiveTable.HivePartition;

import java.io.IOException;
import java.util.List;

/**
 * Extension of {@link HiveScan} which support reading Hive tables using Drill's native parquet reader.
 */
@JsonTypeName("hive-drill-native-parquet-scan")
public class HiveDrillNativeParquetScan extends HiveScan {

  @JsonCreator
  public HiveDrillNativeParquetScan(@JsonProperty("userName") String userName,
                                    @JsonProperty("hive-table") HiveReadEntry hiveReadEntry,
                                    @JsonProperty("storage-plugin") String storagePluginName,
                                    @JsonProperty("columns") List<SchemaPath> columns,
                                    @JacksonInject StoragePluginRegistry pluginRegistry) throws ExecutionSetupException {
    super(userName, hiveReadEntry, storagePluginName, columns, pluginRegistry);
  }

  public HiveDrillNativeParquetScan(String userName, HiveReadEntry hiveReadEntry, HiveStoragePlugin storagePlugin,
      List<SchemaPath> columns, HiveMetadataProvider metadataProvider) throws ExecutionSetupException {
    super(userName, hiveReadEntry, storagePlugin, columns, metadataProvider);
  }

  public HiveDrillNativeParquetScan(final HiveScan hiveScan) {
    super(hiveScan);
  }

  @Override
  public ScanStats getScanStats() {
    final ScanStats nativeHiveScanStats = super.getScanStats();

    // As Drill's native parquet record reader is faster and memory efficient. Divide the CPU cost
    // by a factor to let the planner choose HiveDrillNativeScan over HiveScan with SerDes.
    return new ScanStats(
        nativeHiveScanStats.getGroupScanProperty(),
        nativeHiveScanStats.getRecordCount(),
        nativeHiveScanStats.getCpuCost()/getSerDeOverheadFactor(),
        nativeHiveScanStats.getDiskCost());
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    try {
      return new HiveDrillNativeParquetSubScan((HiveSubScan)super.getSpecificScan(minorFragmentId));
    } catch (IOException | ReflectiveOperationException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public boolean isNativeReader() {
    return true;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    return new HiveDrillNativeParquetScan(this);
  }

  @Override
  public HiveScan clone(HiveReadEntry hiveReadEntry) throws ExecutionSetupException {
    return new HiveDrillNativeParquetScan(getUserName(), hiveReadEntry, storagePlugin, columns, metadataProvider);
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    final HiveDrillNativeParquetScan scan = new HiveDrillNativeParquetScan(this);
    scan.columns = columns;
    return scan;
  }

  @Override
  public String toString() {
    final List<HivePartition> partitions = hiveReadEntry.getHivePartitionWrappers();
    int numPartitions = partitions == null ? 0 : partitions.size();
    return "HiveDrillNativeParquetScan [table=" + hiveReadEntry.getHiveTableWrapper()
        + ", columns=" + columns
        + ", numPartitions=" + numPartitions
        + ", partitions= " + partitions
        + ", inputDirectories=" + metadataProvider.getInputDirectories(hiveReadEntry) + "]";
  }
}
