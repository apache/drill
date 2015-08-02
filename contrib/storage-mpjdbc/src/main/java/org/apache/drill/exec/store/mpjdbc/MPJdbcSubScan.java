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
package org.apache.drill.exec.store.mpjdbc;

import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.ischema.SelectedTable;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

public class MPJdbcSubScan extends AbstractBase implements SubScan {
  private MPJdbcFormatPlugin plugin;
  private MPJdbcFormatConfig pluginConfig;
  private List<SchemaPath> columns;
  private List<MPJdbcScanSpec> scanList;
  private String userName;

  @JsonCreator
  public MPJdbcSubScan(@JacksonInject StoragePluginRegistry registry,
      @JsonProperty("userName") String userName,
      @JsonProperty("pluginConfig") MPJdbcFormatConfig pluginConfig,
      @JsonProperty("ScanList") List<MPJdbcScanSpec> scanlist,
      @JsonProperty("columns") List<SchemaPath> columns) throws ExecutionSetupException {
    super(userName);
    this.plugin = (MPJdbcFormatPlugin) registry.getPlugin(pluginConfig);
    this.pluginConfig = pluginConfig;
    this.columns = columns;
    this.scanList = scanlist;
    this.userName = userName;
  }
  public MPJdbcSubScan(MPJdbcFormatPlugin plugin,
          @JsonProperty("userName") String userName,
          @JsonProperty("pluginConfig") MPJdbcFormatConfig pluginConfig,
          @JsonProperty("ScanList") List<MPJdbcScanSpec> scanlist,
          @JsonProperty("columns") List<SchemaPath> columns) {
    super(userName);
    this.plugin = plugin;
    this.pluginConfig = pluginConfig;
    this.columns = columns;
    this.scanList = scanlist;
    this.userName = userName;
    }

  @Override
  public int getOperatorType() {
    return 55;
  }

  @JsonProperty("columns")
  public List<SchemaPath> getColumns() {
    // TODO Auto-generated method stub
    return columns;
  }

  @JsonIgnore
  public List<MPJdbcScanSpec> getScanList() {
    return this.scanList;
  }

  @JsonIgnore
  public MPJdbcFormatConfig getConfig() {
    return this.pluginConfig;
  }

  @JsonIgnore
  public MPJdbcFormatPlugin getPlugin() {
    return this.plugin;
  }

  @Override
  public boolean isExecutable() {
    return false;
  }

  @Override
  public <T, X, E extends Throwable> T accept(
      PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new MPJdbcSubScan(plugin,userName, pluginConfig, scanList, columns);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }
}
