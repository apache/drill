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
package org.apache.drill.exec.store.maprdb;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.hbase.HBaseSubScan;
import org.apache.drill.exec.store.hbase.HBaseUtils;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.CharacterCodingException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

// Class containing information for reading a single HBase region
@JsonTypeName("maprdb-sub-scan")
public class MapRDBSubScan extends AbstractBase implements SubScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MapRDBSubScan.class);

  @JsonProperty
  public final StoragePluginConfig storage;
  @JsonIgnore
  private final FileSystemPlugin fsStoragePlugin;
  private final List<HBaseSubScan.HBaseSubScanSpec> regionScanSpecList;
  private final List<SchemaPath> columns;

  @JsonCreator
  public MapRDBSubScan(@JacksonInject StoragePluginRegistry registry,
                      @JsonProperty("storage") StoragePluginConfig storage,
                      @JsonProperty("regionScanSpecList") LinkedList<HBaseSubScan.HBaseSubScanSpec> regionScanSpecList,
                      @JsonProperty("columns") List<SchemaPath> columns) throws ExecutionSetupException {
    this.fsStoragePlugin = (FileSystemPlugin) registry.getPlugin(storage);
    this.regionScanSpecList = regionScanSpecList;
    this.storage = storage;
    this.columns = columns;
  }

    public MapRDBSubScan(FileSystemPlugin storagePlugin, StoragePluginConfig config,
                         List<HBaseSubScan.HBaseSubScanSpec> hBaseSubScanSpecs, List<SchemaPath> columns) {
        fsStoragePlugin = storagePlugin;
        storage = config;
        this.regionScanSpecList = hBaseSubScanSpecs;
        this.columns = columns;
    }

    public List<HBaseSubScan.HBaseSubScanSpec> getRegionScanSpecList() {
    return regionScanSpecList;
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  @Override
  public boolean isExecutable() {
    return false;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new MapRDBSubScan(fsStoragePlugin, storage, regionScanSpecList, columns);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  public static class HBaseSubScanSpec {

    protected String tableName;
    protected String regionServer;
    protected byte[] startRow;
    protected byte[] stopRow;
    protected byte[] serializedFilter;

    @parquet.org.codehaus.jackson.annotate.JsonCreator
    public HBaseSubScanSpec(@JsonProperty("tableName") String tableName,
                            @JsonProperty("regionServer") String regionServer,
                            @JsonProperty("startRow") byte[] startRow,
                            @JsonProperty("stopRow") byte[] stopRow,
                            @JsonProperty("serializedFilter") byte[] serializedFilter,
                            @JsonProperty("filterString") String filterString) {
      if (serializedFilter != null && filterString != null) {
        throw new IllegalArgumentException("The parameters 'serializedFilter' or 'filterString' cannot be specified at the same time.");
      }
      this.tableName = tableName;
      this.regionServer = regionServer;
      this.startRow = startRow;
      this.stopRow = stopRow;
      if (serializedFilter != null) {
        this.serializedFilter = serializedFilter;
      } else {
        this.serializedFilter = HBaseUtils.serializeFilter(parseFilterString(filterString));
      }
    }

    static final ParseFilter PARSE_FILTER = new ParseFilter();

    static Filter parseFilterString(String filterString) {
      if (filterString == null) {
        return null;
      }
      try {
        return PARSE_FILTER.parseFilterString(filterString);
      } catch (CharacterCodingException e) {
        throw new DrillRuntimeException("Error parsing filter string: " + filterString, e);
      }
    }

    /* package */ HBaseSubScanSpec() {
      // empty constructor, to be used with builder pattern;
    }

    @JsonIgnore
    private Filter scanFilter;
    public Filter getScanFilter() {
      if (scanFilter == null &&  serializedFilter != null) {
          scanFilter = HBaseUtils.deserializeFilter(serializedFilter);
      }
      return scanFilter;
    }

    public String getTableName() {
      return tableName;
    }

    public HBaseSubScanSpec setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public String getRegionServer() {
      return regionServer;
    }

    public HBaseSubScanSpec setRegionServer(String regionServer) {
      this.regionServer = regionServer;
      return this;
    }

    public byte[] getStartRow() {
      return startRow;
    }

    public HBaseSubScanSpec setStartRow(byte[] startRow) {
      this.startRow = startRow;
      return this;
    }

    public byte[] getStopRow() {
      return stopRow;
    }

    public HBaseSubScanSpec setStopRow(byte[] stopRow) {
      this.stopRow = stopRow;
      return this;
    }

    public byte[] getSerializedFilter() {
      return serializedFilter;
    }

    public HBaseSubScanSpec setSerializedFilter(byte[] serializedFilter) {
      this.serializedFilter = serializedFilter;
      this.scanFilter = null;
      return this;
    }

    @Override
    public String toString() {
      return "HBaseScanSpec [tableName=" + tableName
          + ", startRow=" + (startRow == null ? null : Bytes.toStringBinary(startRow))
          + ", stopRow=" + (stopRow == null ? null : Bytes.toStringBinary(stopRow))
          + ", filter=" + (getScanFilter() == null ? null : getScanFilter().toString())
          + ", regionServer=" + regionServer + "]";
    }

  }

  @Override
  public int getOperatorType() {
    return 1001;
  }

}
