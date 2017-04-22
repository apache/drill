/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.openTSDB;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.openTSDB.OpenTSDBSubScan.OpenTSDBSubScanSpec;
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.CompleteWork;
import org.apache.drill.exec.store.schedule.EndpointByteMap;
import org.apache.drill.exec.store.schedule.EndpointByteMapImpl;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Slf4j
@JsonTypeName("openTSDB-scan")
public class OpenTSDBGroupScan extends AbstractGroupScan {

    private static final long DEFAULT_TABLET_SIZE = 1000;

    private OpenTSDBStoragePluginConfig storagePluginConfig;
    private OpenTSDBScanSpec openTSDBScanSpec;
    private OpenTSDBStoragePlugin storagePlugin;
    private boolean filterPushedDown = false;
    private ListMultimap<Integer, OpenTSDBWork> assignments;
    private List<SchemaPath> columns;
    private List<OpenTSDBWork> openTSDBWorkList = Lists.newArrayList();
    private List<EndpointAffinity> affinities;

    @JsonCreator
    public OpenTSDBGroupScan(@JsonProperty("openTSDBScanSpec") OpenTSDBScanSpec openTSDBScanSpec,
                             @JsonProperty("storage") OpenTSDBStoragePluginConfig openTSDBStoragePluginConfig,
                             @JsonProperty("columns") List<SchemaPath> columns,
                             @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
        this((OpenTSDBStoragePlugin) pluginRegistry.getPlugin(openTSDBStoragePluginConfig), openTSDBScanSpec, columns);
    }

    public OpenTSDBGroupScan(OpenTSDBStoragePlugin storagePlugin,
                             OpenTSDBScanSpec scanSpec, List<SchemaPath> columns) {
        super((String) null);
        this.storagePlugin = storagePlugin;
        this.storagePluginConfig = storagePlugin.getConfig();
        this.openTSDBScanSpec = scanSpec;
        this.columns = columns == null || columns.size() == 0 ? ALL_COLUMNS : columns;
        init();
    }

    private void init() {
        Collection<DrillbitEndpoint> endpoints = storagePlugin.getContext().getBits();
        Map<String, DrillbitEndpoint> endpointMap = Maps.newHashMap();

        for (DrillbitEndpoint endpoint : endpoints) {
            endpointMap.put(endpoint.getAddress(), endpoint);
        }
    }

    private static class OpenTSDBWork implements CompleteWork {

        private EndpointByteMapImpl byteMap = new EndpointByteMapImpl();

        @Override
        public long getTotalBytes() {
            return DEFAULT_TABLET_SIZE;
        }

        @Override
        public EndpointByteMap getByteMap() {
            return byteMap;
        }

        @Override
        public int compareTo(CompleteWork o) {
            return 0;
        }
    }

    /**
     * Private constructor, used for cloning.
     *
     * @param that The OpenTSDBGroupScan to clone
     */
    public OpenTSDBGroupScan(OpenTSDBGroupScan that) {
        super((String) null);
        this.columns = that.columns;
        this.openTSDBScanSpec = that.openTSDBScanSpec;
        this.storagePlugin = that.storagePlugin;
        this.storagePluginConfig = that.storagePluginConfig;
        this.filterPushedDown = that.filterPushedDown;
        this.openTSDBWorkList = that.openTSDBWorkList;
        this.assignments = that.assignments;
        init();
    }

    @Override
    public GroupScan clone(List<SchemaPath> columns) {
        OpenTSDBGroupScan newScan = new OpenTSDBGroupScan(this);
        newScan.columns = columns;
        return newScan;
    }

    @Override
    public List<EndpointAffinity> getOperatorAffinity() {
        if (affinities == null) {
            affinities = AffinityCreator.getAffinityMap(openTSDBWorkList);
        }
        return affinities;
    }

    @Override
    public int getMaxParallelizationWidth() {
        return openTSDBWorkList.size();
    }

    // TODO: Check this getMappings method
    @Override
    public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) {
        assignments = AssignmentCreator.getMappings(incomingEndpoints, openTSDBWorkList);
    }

    @Override
    public OpenTSDBSubScan getSpecificScan(int minorFragmentId) {
        List<OpenTSDBSubScanSpec> scanSpecList = Lists.newArrayList();
        scanSpecList.add(new OpenTSDBSubScanSpec(getTableName()));
        return new OpenTSDBSubScan(storagePlugin, storagePluginConfig, scanSpecList, this.columns);
    }

    @Override
    public ScanStats getScanStats() {
        long recordCount = 100000 * openTSDBWorkList.size();
        return new ScanStats(ScanStats.GroupScanProperty.NO_EXACT_ROW_COUNT, recordCount, 1, recordCount);
    }

    @Override
    @JsonIgnore
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
        Preconditions.checkArgument(children.isEmpty());
        return new OpenTSDBGroupScan(this);
    }

    @JsonIgnore
    public OpenTSDBStoragePlugin getStoragePlugin() {
        return storagePlugin;
    }

    @JsonIgnore
    public String getTableName() {
        return getOpenTSDBScanSpec().getTableName();
    }


    @Override
    public String getDigest() {
        return toString();
    }

    @Override
    public String toString() {
        return "OpenTSDBGroupScan [OpenTSDBScanSpec="
                + openTSDBScanSpec + "]";
    }

    @JsonProperty("storage")
    public OpenTSDBStoragePluginConfig getStorageConfig() {
        return this.storagePluginConfig;
    }

    @JsonProperty
    public List<SchemaPath> getColumns() {
        return columns;
    }

    @JsonProperty
    public OpenTSDBScanSpec getOpenTSDBScanSpec() {
        return openTSDBScanSpec;
    }

    @Override
    @JsonIgnore
    public boolean canPushdownProjects(List<SchemaPath> columns) {
        return true;
    }

    @JsonIgnore
    public void setFilterPushedDown(boolean b) {
        this.filterPushedDown = true;
    }

    @JsonIgnore
    public boolean isFilterPushedDown() {
        return filterPushedDown;
    }


    /**
     * Empty constructor, do not use, only for testing.
     */
    @VisibleForTesting
    public OpenTSDBGroupScan() {
        super((String) null);
    }

    /**
     * Do not use, only for testing.
     */
    @VisibleForTesting
    public void setOpenTSDBScanSpec(OpenTSDBScanSpec openTSDBScanSpec) {
        this.openTSDBScanSpec = openTSDBScanSpec;
    }

}
