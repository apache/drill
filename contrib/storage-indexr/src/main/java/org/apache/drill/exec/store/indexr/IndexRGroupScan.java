/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.indexr;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.planner.fragment.DistributionAffinity;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.indexr.LocalFirstAssigner.EndpointAssignment;
import org.apache.drill.exec.store.indexr.ScanWrokProvider.FragmentAssignment;
import org.apache.drill.exec.store.indexr.ScanWrokProvider.Stat;
import org.apache.drill.exec.store.indexr.ScanWrokProvider.Works;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import io.indexr.segment.helper.RangeWork;
import io.indexr.server.HybridTable;

@JsonTypeName("indexr-scan")
public class IndexRGroupScan extends AbstractGroupScan {
  private static final Logger logger = LoggerFactory.getLogger(IndexRGroupScan.class);
  private static final boolean USE_LOCAL_FIRST_ASSIGNER = false;

  private final IndexRStoragePlugin plugin;
  private final String scanId;
  private final List<SchemaPath> columns;
  private final long limitScanRows;

  private IndexRScanSpec scanSpec;
  private long scanRowCount;
  private HashMap<Integer, FragmentAssignment> assignments;

  @JsonCreator
  public IndexRGroupScan(
      @JsonProperty("userName") String userName,//
      @JsonProperty("indexrScanSpec") IndexRScanSpec scanSpec,//
      @JsonProperty("storage") IndexRStoragePluginConfig storagePluginConfig,//
      @JsonProperty("columns") List<SchemaPath> columns,//
      @JsonProperty("limitScanRows") long limitScanRows,//
      @JsonProperty("scanId") String scanId,//
      @JacksonInject StoragePluginRegistry pluginRegistry//
  ) throws IOException, ExecutionSetupException {
    this(userName,
        (IndexRStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig),
        scanSpec,
        columns,
        limitScanRows,
        scanId);
  }

  public IndexRGroupScan(String userName,
                         IndexRStoragePlugin plugin,
                         IndexRScanSpec scanSpec,
                         List<SchemaPath> columns,
                         long limitScanRows,
                         String scanId) {
    super(userName);
    this.plugin = plugin;
    this.scanSpec = scanSpec;
    this.scanId = scanId;
    this.columns = columns;
    this.limitScanRows = limitScanRows;

    this.scanRowCount = calStat().scanRowCount;
  }

  /**
   * Private constructor, used for cloning.
   */
  private IndexRGroupScan(IndexRGroupScan that,
                          List<SchemaPath> columns,
                          long limitScanRows,
                          long scanRowCount) {
    super(that);
    this.plugin = that.plugin;
    this.scanSpec = that.scanSpec;
    this.scanId = that.scanId;

    this.columns = columns;
    this.limitScanRows = limitScanRows;
    this.scanRowCount = scanRowCount;
  }

  public void setScanSpec(IndexRScanSpec scanSpec) {
    this.scanSpec = scanSpec;
    this.scanRowCount = calStat().scanRowCount;
  }

  private Stat calStat() {
    return ScanWrokProvider.getStat(
        plugin,
        scanSpec,
        scanId,
        limitScanRows,
        columns);
  }

  private Works calWorks(boolean must) {
    return ScanWrokProvider.getScanWorks(
        must,
        plugin,
        scanSpec,
        scanId,
        limitScanRows,
        columns);
  }

  @Override
  public IndexRGroupScan clone(List<SchemaPath> columns) {
    return new IndexRGroupScan(
        this,
        columns,
        this.limitScanRows,
        this.scanRowCount);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    return new IndexRGroupScan(
        this,
        this.columns,
        this.limitScanRows,
        this.scanRowCount);
  }

  @Override
  public GroupScan applyLimit(long maxRecords) {
    if (this.limitScanRows != Long.MAX_VALUE) {
      // Applied already.
      return null;
    }
    logger.debug("=============== applyLimit maxRecords:{}", maxRecords);
    try {
      long newScanRowCount = ScanWrokProvider.getStat(
          this.plugin,
          this.scanSpec,
          this.scanId,
          maxRecords, // the new one.
          this.columns).scanRowCount;
      if (newScanRowCount < this.scanRowCount) {
        return new IndexRGroupScan(
            this,
            this.columns,
            maxRecords,
            newScanRowCount);
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private int colCount(HybridTable table) {
    int colCount = 0;
    if (columns == null) {
      colCount = 20;
    } else if (AbstractRecordReader.isStarQuery(columns)) {
      colCount = table.schema().schema.columns.size();
    } else {
      colCount = columns.size();
    }

    return colCount;
  }

  @Override
  public ScanStats getScanStats() {
    try {
      HybridTable table = plugin.indexRNode().getTablePool().get(scanSpec.getTableName());
      logger.debug("=============== getScanStats, scanRowCount: {}", scanRowCount);

      // Ugly hack!
      if (scanRowCount <= ExecConstants.SLICE_TARGET_DEFAULT) {

        // We must make the planner use exchange which can spreads the query fragments among nodes.
        // Otherwise realtime segments won't be able to query.
        // We keep the scan rows over a threshold to acheive this.
        // TODO Somebody please get a better idea ...

        long useRowCount = ExecConstants.SLICE_TARGET_DEFAULT;
        return new ScanStats(ScanStats.GroupScanProperty.NO_EXACT_ROW_COUNT, useRowCount, 1, useRowCount * colCount(table));
      } else {
        return new ScanStats(ScanStats.GroupScanProperty.EXACT_ROW_COUNT, scanRowCount, 1, scanRowCount * colCount(table));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return calWorks(true).endpointAffinities;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
    try {
      Works works = calWorks(true);
      List<ScanCompleteWork> historyWorks = works.historyWorks;
      Map<DrillbitEndpoint, List<ScanCompleteWork>> realtimeWorks = works.realtimeWorks;
      for (DrillbitEndpoint endpoint : realtimeWorks.keySet()) {
        if (!endpoints.contains(endpoint)) {
          String errorMsg = String.format(//
              "Realtime works on %s cannot be assigned, because thery are not in work list %s.",//
              endpoint.getAddress(),//
              Lists.transform(endpoints, new Function<DrillbitEndpoint, String>() {
                @Nullable
                @Override
                public String apply(@Nullable DrillbitEndpoint input) {
                  return input.getAddress();
                }
              }));
          throw new IllegalStateException(errorMsg);
        }
      }

      ListMultimap<DrillbitEndpoint, RangeWork> endpointToWorks = ArrayListMultimap.create();

      if (USE_LOCAL_FIRST_ASSIGNER) {
        List<ScanCompleteWork> allWorks = new ArrayList<>(historyWorks);
        for (DrillbitEndpoint endpoint : realtimeWorks.keySet()) {
          allWorks.addAll(realtimeWorks.get(endpoint));
        }
        Map<DrillbitEndpoint, EndpointAssignment> fakeAssignments = new LocalFirstAssigner(endpoints, allWorks).assign();
        for (EndpointAssignment assignment : fakeAssignments.values()) {
          endpointToWorks.putAll(assignment.endpoint, assignment.works);
        }
      } else {

        // Put history works.
        ListMultimap<Integer, ScanCompleteWork> fakeAssignments = AssignmentCreator.getMappings(endpoints, historyWorks);
        for (int id = 0; id < endpoints.size(); id++) {
          DrillbitEndpoint endpoint = endpoints.get(id);
          endpointToWorks.putAll(endpoint, fakeAssignments.get(id));
        }
        // Put reatlime works.
        for (DrillbitEndpoint endpoint : realtimeWorks.keySet()) {
          endpointToWorks.putAll(endpoint, realtimeWorks.get(endpoint));
        }
      }

      ListMultimap<DrillbitEndpoint, Integer> endpointToMinoFragmentId = ArrayListMultimap.create();
      HashMap<Integer, FragmentAssignment> assignments = new HashMap<>();
      for (int id = 0; id < endpoints.size(); id++) {
        endpointToMinoFragmentId.put(endpoints.get(id), id);
      }
      for (int id = 0; id < endpoints.size(); id++) {
        DrillbitEndpoint endpoint = endpoints.get(id);

        List<RangeWork> epWorks = endpointToWorks.get(endpoint);
        List<Integer> fragments = endpointToMinoFragmentId.get(endpoint);

        epWorks = RangeWork.compact(new ArrayList<>(epWorks));
        assignments.put(id, new FragmentAssignment(fragments.size(), fragments.indexOf(id), epWorks));
      }

      this.assignments = assignments;

      logger.debug("=====================  applyAssignments endpoints:{}", endpoints);
      logger.debug("=====================  applyAssignments endpointToWorks:{}", endpointToWorks);
      logger.debug("=====================  applyAssignments assignments:{}", assignments);

    } catch (Exception e) {
      throw new PhysicalOperatorSetupException(e);
    }
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    FragmentAssignment assign = assignments.get(minorFragmentId);

    IndexRSubScanSpec subScanSpec = new IndexRSubScanSpec(//
        scanId,//
        scanSpec.getTableName(),//
        assign.fragmentCount,//
        assign.fragmentIndex, //
        assign.endpointWorks,//
        scanSpec.getRSFilter());
    return new IndexRSubScan(plugin, subScanSpec, columns);
  }

  @Override
  @JsonIgnore
  public int getMinParallelizationWidth() {
    try {
      Works works = calWorks(false);
      int pw = works != null ? works.minPw : calStat().minPw;
      logger.debug("=============== getMinParallelizationWidth {}", pw);
      return pw;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @JsonIgnore
  public int getMaxParallelizationWidth() {
    try {
      Works works = calWorks(false);
      int pw = works != null ? works.maxPw : calStat().maxPw;
      logger.debug("=============== getMaxParallelizationWidth {}", pw);
      return pw;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // =================================
  // Fields and simple methods overrided.

  @JsonIgnore
  public IndexRStoragePlugin getStoragePlugin() {
    return plugin;
  }

  @JsonProperty("storage")
  public IndexRStoragePluginConfig getStorageConfig() {
    return plugin.getConfig();
  }

  @JsonProperty("columns")
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty("indexrScanSpec")
  public IndexRScanSpec getScanSpec() {
    return scanSpec;
  }

  @JsonProperty("scanId")
  public String getScanId() {
    return scanId;
  }

  @JsonProperty("limitScanRows")
  public long getLimitScanRows() {
    return limitScanRows;
  }

  @Override
  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  @Override
  @JsonIgnore
  public boolean supportsLimitPushdown() {
    return true;
  }

  @Override
  @JsonIgnore
  public String getDigest() {
    return toString();
  }

  @Override
  public DistributionAffinity getDistributionAffinity() {
    return DistributionAffinity.HARD;
  }

  @Override
  @JsonIgnore
  public String toString() {
    return String.format("IndexRGroupScan@%s{Spec=%s, columns=%s}",
        Integer.toHexString(super.hashCode()),
        scanSpec,
        columns);
  }
}
