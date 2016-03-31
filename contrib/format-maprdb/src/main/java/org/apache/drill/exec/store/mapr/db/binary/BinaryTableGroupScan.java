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
package org.apache.drill.exec.store.mapr.db.binary;

import static org.apache.drill.exec.store.mapr.db.util.CommonFns.isNullOrEmpty;

import java.io.IOException;
import java.util.List;
import java.util.TreeMap;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.hbase.DrillHBaseConstants;
import org.apache.drill.exec.store.hbase.HBaseScanSpec;
import org.apache.drill.exec.store.mapr.db.MapRDBFormatPlugin;
import org.apache.drill.exec.store.mapr.db.MapRDBFormatPluginConfig;
import org.apache.drill.exec.store.mapr.db.MapRDBGroupScan;
import org.apache.drill.exec.store.mapr.db.MapRDBSubScan;
import org.apache.drill.exec.store.mapr.db.MapRDBSubScanSpec;
import org.apache.drill.exec.store.mapr.db.MapRDBTableStats;
import org.apache.drill.exec.store.mapr.db.TabletFragmentInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.codehaus.jackson.annotate.JsonCreator;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

@JsonTypeName("maprdb-binary-scan")
public class BinaryTableGroupScan extends MapRDBGroupScan implements DrillHBaseConstants {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BinaryTableGroupScan.class);

  public static final String TABLE_BINARY = "binary";

  private HBaseScanSpec hbaseScanSpec;

  private HTableDescriptor hTableDesc;

  private MapRDBTableStats tableStats;

  @JsonCreator
  public BinaryTableGroupScan(@JsonProperty("userName") final String userName,
                              @JsonProperty("hbaseScanSpec") HBaseScanSpec scanSpec,
                              @JsonProperty("storage") FileSystemConfig storagePluginConfig,
                              @JsonProperty("format") MapRDBFormatPluginConfig formatPluginConfig,
                              @JsonProperty("columns") List<SchemaPath> columns,
                              @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
    this (userName,
          (FileSystemPlugin) pluginRegistry.getPlugin(storagePluginConfig),
          (MapRDBFormatPlugin) pluginRegistry.getFormatPlugin(storagePluginConfig, formatPluginConfig),
          scanSpec, columns);
  }

  public BinaryTableGroupScan(String userName, FileSystemPlugin storagePlugin,
      MapRDBFormatPlugin formatPlugin, HBaseScanSpec scanSpec, List<SchemaPath> columns) {
    super(storagePlugin, formatPlugin, columns, userName);
    this.hbaseScanSpec = scanSpec;
    init();
  }

  /**
   * Private constructor, used for cloning.
   * @param that The HBaseGroupScan to clone
   */
  private BinaryTableGroupScan(BinaryTableGroupScan that) {
    super(that);
    this.hbaseScanSpec = that.hbaseScanSpec;
    this.endpointFragmentMapping = that.endpointFragmentMapping;
    this.hTableDesc = that.hTableDesc;
    this.tableStats = that.tableStats;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    BinaryTableGroupScan newScan = new BinaryTableGroupScan(this);
    newScan.columns = columns;
    newScan.verifyColumns();
    return newScan;
  }

  private void init() {
    logger.debug("Getting region locations");
    TableName tableName = TableName.valueOf(hbaseScanSpec.getTableName());
    try (Admin admin = formatPlugin.getConnection().getAdmin();
         RegionLocator locator = formatPlugin.getConnection().getRegionLocator(tableName)) {
      hTableDesc = admin.getTableDescriptor(tableName);
      tableStats = new MapRDBTableStats(getHBaseConf(), hbaseScanSpec.getTableName());

      boolean foundStartRegion = false;
      regionsToScan = new TreeMap<TabletFragmentInfo, String>();
      List<HRegionLocation> regionLocations = locator.getAllRegionLocations();
      for (HRegionLocation regionLocation : regionLocations) {
        HRegionInfo regionInfo = regionLocation.getRegionInfo();
        if (!foundStartRegion && hbaseScanSpec.getStartRow() != null && hbaseScanSpec.getStartRow().length != 0 && !regionInfo.containsRow(hbaseScanSpec.getStartRow())) {
          continue;
        }
        foundStartRegion = true;
        regionsToScan.put(new TabletFragmentInfo(regionInfo), regionLocation.getHostname());
        if (hbaseScanSpec.getStopRow() != null && hbaseScanSpec.getStopRow().length != 0 && regionInfo.containsRow(hbaseScanSpec.getStopRow())) {
          break;
        }
      }
    } catch (Exception e) {
      throw new DrillRuntimeException("Error getting region info for table: " + hbaseScanSpec.getTableName(), e);
    }
    verifyColumns();
  }

  private void verifyColumns() {
    /*
    if (columns != null) {
      for (SchemaPath column : columns) {
        if (!(column.equals(ROW_KEY_PATH) || hTableDesc.hasFamily(HBaseUtils.getBytes(column.getRootSegment().getPath())))) {
          DrillRuntimeException.format("The column family '%s' does not exist in HBase table: %s .",
              column.getRootSegment().getPath(), hTableDesc.getNameAsString());
        }
      }
    }
    */
  }

  protected MapRDBSubScanSpec getSubScanSpec(TabletFragmentInfo tfi) {
    HBaseScanSpec spec = hbaseScanSpec;
    MapRDBSubScanSpec subScanSpec = new MapRDBSubScanSpec(
        spec.getTableName(),
        regionsToScan.get(tfi),
        (!isNullOrEmpty(spec.getStartRow()) && tfi.containsRow(spec.getStartRow())) ? spec.getStartRow() : tfi.getStartKey(),
        (!isNullOrEmpty(spec.getStopRow()) && tfi.containsRow(spec.getStopRow())) ? spec.getStopRow() : tfi.getEndKey(),
        spec.getSerializedFilter(),
        null);
    return subScanSpec;
  }

  @Override
  public MapRDBSubScan getSpecificScan(int minorFragmentId) {
    assert minorFragmentId < endpointFragmentMapping.size() : String.format(
        "Mappings length [%d] should be greater than minor fragment id [%d] but it isn't.", endpointFragmentMapping.size(),
        minorFragmentId);
    return new MapRDBSubScan(getUserName(), formatPluginConfig, getStoragePlugin(), getStoragePlugin().getConfig(),
        endpointFragmentMapping.get(minorFragmentId), columns, TABLE_BINARY);
  }

  @Override
  public ScanStats getScanStats() {
    //TODO: look at stats for this.
    long rowCount = (long) ((hbaseScanSpec.getFilter() != null ? .5 : 1) * tableStats.getNumRows());
    int avgColumnSize = 10;
    int numColumns = (columns == null || columns.isEmpty()) ? 100 : columns.size();
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, rowCount, 1, avgColumnSize * numColumns * rowCount);
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new BinaryTableGroupScan(this);
  }

  @JsonIgnore
  public Configuration getHBaseConf() {
    return getFormatPlugin().getHBaseConf();
  }

  @JsonIgnore
  public String getTableName() {
    return getHBaseScanSpec().getTableName();
  }

  @Override
  public String toString() {
    return "BinaryTableGroupScan [ScanSpec="
        + hbaseScanSpec + ", columns="
        + columns + "]";
  }

  @JsonProperty
  public HBaseScanSpec getHBaseScanSpec() {
    return hbaseScanSpec;
  }

}
