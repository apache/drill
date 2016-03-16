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
package org.apache.drill.exec.store.kudu;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.store.StoragePluginRegistry;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

// Class containing information for reading a single Kudu tablet
@JsonTypeName("kudu-tablet-scan")
public class KuduSubScan extends AbstractBase implements SubScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KuduSubScan.class);

  @JsonProperty
  public final KuduStoragePluginConfig storage;


  private final KuduStoragePlugin kuduStoragePlugin;
  private final List<KuduSubScanSpec> tabletScanSpecList;
  private final List<SchemaPath> columns;

  @JsonCreator
  public KuduSubScan(@JacksonInject StoragePluginRegistry registry,
                      @JsonProperty("storage") StoragePluginConfig storage,
      @JsonProperty("tabletScanSpecList") LinkedList<KuduSubScanSpec> tabletScanSpecList,
                      @JsonProperty("columns") List<SchemaPath> columns) throws ExecutionSetupException {
    super((String) null);
    kuduStoragePlugin = (KuduStoragePlugin) registry.getPlugin(storage);
    this.tabletScanSpecList = tabletScanSpecList;
    this.storage = (KuduStoragePluginConfig) storage;
    this.columns = columns;
  }

  public KuduSubScan(KuduStoragePlugin plugin, KuduStoragePluginConfig config,
      List<KuduSubScanSpec> tabletInfoList, List<SchemaPath> columns) {
    super((String) null);
    kuduStoragePlugin = plugin;
    storage = config;
    this.tabletScanSpecList = tabletInfoList;
    this.columns = columns;
  }

  public List<KuduSubScanSpec> getTabletScanSpecList() {
    return tabletScanSpecList;
  }

  @JsonIgnore
  public KuduStoragePluginConfig getStorageConfig() {
    return storage;
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  @Override
  public boolean isExecutable() {
    return false;
  }

  @JsonIgnore
  public KuduStoragePlugin getStorageEngine(){
    return kuduStoragePlugin;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new KuduSubScan(kuduStoragePlugin, storage, tabletScanSpecList, columns);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  public static class KuduSubScanSpec {

    private final String tableName;
    private final byte[] startKey;
    private final byte[] endKey;

    @JsonCreator
    public KuduSubScanSpec(@JsonProperty("tableName") String tableName,
                           @JsonProperty("startKey") byte[] startKey,
                           @JsonProperty("endKey") byte[] endKey) {
      this.tableName = tableName;
      this.startKey = startKey;
      this.endKey = endKey;
    }

    public String getTableName() {
      return tableName;
    }

    public byte[] getStartKey() {
      return startKey;
    }

    public byte[] getEndKey() {
      return endKey;
    }

  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.HBASE_SUB_SCAN_VALUE;
  }

}
