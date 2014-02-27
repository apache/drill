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
package org.apache.drill.exec.store.hbase;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.physical.base.SubScan;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.drill.exec.store.StoragePluginRegistry;

// Class containing information for reading a single HBase row group form HDFS
@JsonTypeName("hbase-row-group-scan")
public class HBaseSubScan extends AbstractBase implements SubScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseSubScan.class);

  @JsonProperty
  public final StoragePluginConfig storage;
  @JsonIgnore
  private final HBaseStoragePlugin hbaseStoragePlugin;
  private final List<HBaseSubScanReadEntry> rowGroupReadEntries;
  private final List<SchemaPath> columns;

  @JsonCreator
  public HBaseSubScan(@JacksonInject StoragePluginRegistry registry, @JsonProperty("storage") StoragePluginConfig storage,
                      @JsonProperty("rowGroupReadEntries") LinkedList<HBaseSubScanReadEntry> rowGroupReadEntries,
                      @JsonProperty("columns") List<SchemaPath> columns) throws ExecutionSetupException {
    hbaseStoragePlugin = (HBaseStoragePlugin) registry.getEngine(storage);
    this.rowGroupReadEntries = rowGroupReadEntries;
    this.storage = storage;
    this.columns = columns;
  }

  public HBaseSubScan(HBaseStoragePlugin plugin, HBaseStoragePluginConfig config,
                      List<HBaseSubScanReadEntry> regionInfoList,
                      List<SchemaPath> columns) {
    hbaseStoragePlugin = plugin;
    storage = config;
    this.rowGroupReadEntries = regionInfoList;
    this.columns = columns;
  }

  public List<HBaseSubScanReadEntry> getRowGroupReadEntries() {
    return rowGroupReadEntries;
  }

  @JsonIgnore
  public StoragePluginConfig getStorageConfig() {
    return storage;
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  @Override
  public OperatorCost getCost() {
    return null;
  }

  @Override
  public Size getSize() {
    return null;
  }

  @Override
  public boolean isExecutable() {
    return false;
  }

  @JsonIgnore
  public HBaseStoragePlugin getStorageEngine(){
    return hbaseStoragePlugin;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new HBaseSubScan(hbaseStoragePlugin, (HBaseStoragePluginConfig) storage, rowGroupReadEntries, columns);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  public static class HBaseSubScanReadEntry {

    private String tableName;
    private String startRow;
    private String endRow;

    @parquet.org.codehaus.jackson.annotate.JsonCreator
    public HBaseSubScanReadEntry(@JsonProperty("tableName") String tableName,
                                 @JsonProperty("startRow") String startRow, @JsonProperty("endRow") String endRow) {
      this.tableName = tableName;
      this.startRow = startRow;
      this.endRow = endRow;
    }

    public String getTableName() {
      return tableName;
    }

    public String getStartRow() {
      return startRow;
    }

    public String getEndRow() {
      return endRow;
    }
  }

}
