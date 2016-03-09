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
package org.apache.drill.exec.store.mapr.db.json;

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
import org.apache.drill.exec.store.mapr.db.MapRDBFormatPlugin;
import org.apache.drill.exec.store.mapr.db.MapRDBFormatPluginConfig;
import org.apache.drill.exec.store.mapr.db.MapRDBGroupScan;
import org.apache.drill.exec.store.mapr.db.MapRDBSubScan;
import org.apache.drill.exec.store.mapr.db.MapRDBTableStats;
import org.apache.drill.exec.store.mapr.db.TabletFragmentInfo;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.annotate.JsonCreator;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import com.mapr.db.TabletInfo;
import com.mapr.db.impl.TabletInfoImpl;

@JsonTypeName("maprdb-json-scan")
public class JsonTableGroupScan extends MapRDBGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonTableGroupScan.class);

  public static final String TABLE_JSON = "json";

  private MapRDBTableStats tableStats;

  private JsonScanSpec scanSpec;

  @JsonCreator
  public JsonTableGroupScan(@JsonProperty("userName") final String userName,
                            @JsonProperty("scanSpec") JsonScanSpec scanSpec,
                            @JsonProperty("storage") FileSystemConfig storagePluginConfig,
                            @JsonProperty("format") MapRDBFormatPluginConfig formatPluginConfig,
                            @JsonProperty("columns") List<SchemaPath> columns,
                            @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
    this (userName,
          (FileSystemPlugin) pluginRegistry.getPlugin(storagePluginConfig),
          (MapRDBFormatPlugin) pluginRegistry.getFormatPlugin(storagePluginConfig, formatPluginConfig),
          scanSpec, columns);
  }

  public JsonTableGroupScan(String userName, FileSystemPlugin storagePlugin,
                            MapRDBFormatPlugin formatPlugin, JsonScanSpec scanSpec, List<SchemaPath> columns) {
    super(storagePlugin, formatPlugin, columns, userName);
    this.scanSpec = scanSpec;
    init();
  }

  /**
   * Private constructor, used for cloning.
   * @param that The HBaseGroupScan to clone
   */
  private JsonTableGroupScan(JsonTableGroupScan that) {
    super(that);
    this.scanSpec = that.scanSpec;
    this.endpointFragmentMapping = that.endpointFragmentMapping;
    this.tableStats = that.tableStats;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    JsonTableGroupScan newScan = new JsonTableGroupScan(this);
    newScan.columns = columns;
    return newScan;
  }

  private void init() {
    logger.debug("Getting tablet locations");
    try {
      Configuration conf = new Configuration();
      Table t = MapRDB.getTable(scanSpec.getTableName());
      TabletInfo[] tabletInfos = t.getTabletInfos(scanSpec.getCondition());
      tableStats = new MapRDBTableStats(conf, scanSpec.getTableName());

      boolean foundStartRegion = false;
      regionsToScan = new TreeMap<TabletFragmentInfo, String>();
      for (TabletInfo tabletInfo : tabletInfos) {
        TabletInfoImpl tabletInfoImpl = (TabletInfoImpl) tabletInfo;
        if (!foundStartRegion
            && !isNullOrEmpty(scanSpec.getStartRow())
            && !tabletInfoImpl.containsRow(scanSpec.getStartRow())) {
          continue;
        }
        foundStartRegion = true;
        regionsToScan.put(new TabletFragmentInfo(tabletInfoImpl), tabletInfo.getLocations()[0]);
        if (!isNullOrEmpty(scanSpec.getStopRow())
            && tabletInfoImpl.containsRow(scanSpec.getStopRow())) {
          break;
        }
      }
    } catch (Exception e) {
      throw new DrillRuntimeException("Error getting region info for table: " + scanSpec.getTableName(), e);
    }
  }

  protected JsonSubScanSpec getSubScanSpec(TabletFragmentInfo tfi) {
    // XXX/TODO check filter/Condition
    JsonScanSpec spec = scanSpec;
    JsonSubScanSpec subScanSpec = new JsonSubScanSpec(
        spec.getTableName(),
        regionsToScan.get(tfi),
        (!isNullOrEmpty(spec.getStartRow()) && tfi.containsRow(spec.getStartRow())) ? spec.getStartRow() : tfi.getStartKey(),
        (!isNullOrEmpty(spec.getStopRow()) && tfi.containsRow(spec.getStopRow())) ? spec.getStopRow() : tfi.getEndKey(),
        spec.getCondition());
    return subScanSpec;
  }

  @Override
  public MapRDBSubScan getSpecificScan(int minorFragmentId) {
    assert minorFragmentId < endpointFragmentMapping.size() : String.format(
        "Mappings length [%d] should be greater than minor fragment id [%d] but it isn't.", endpointFragmentMapping.size(),
        minorFragmentId);
    return new MapRDBSubScan(getUserName(), formatPluginConfig, getStoragePlugin(), getStoragePlugin().getConfig(),
        endpointFragmentMapping.get(minorFragmentId), columns, TABLE_JSON);
  }

  @Override
  public ScanStats getScanStats() {
    //TODO: look at stats for this.
    long rowCount = (long) ((scanSpec.getSerializedFilter() != null ? .5 : 1) * tableStats.getNumRows());
    int avgColumnSize = 10;
    int numColumns = (columns == null || columns.isEmpty()) ? 100 : columns.size();
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, rowCount, 1, avgColumnSize * numColumns * rowCount);
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new JsonTableGroupScan(this);
  }

  @JsonIgnore
  public String getTableName() {
    return scanSpec.getTableName();
  }

  public boolean isDisablePushdown() {
    return !formatPluginConfig.isEnablePushdown();
  }

  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return formatPluginConfig.isEnablePushdown();
  }

  @Override
  public String toString() {
    return "JsonTableGroupScan [ScanSpec=" + scanSpec + ", columns=" + columns + "]";
  }

  public JsonScanSpec getScanSpec() {
    return scanSpec;
  }

}
