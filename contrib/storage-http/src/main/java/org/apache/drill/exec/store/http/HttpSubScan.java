/*
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
package org.apache.drill.exec.store.http;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;

@JsonTypeName("http-sub-scan")
public class HttpSubScan extends AbstractBase implements SubScan {

  private final HttpScanSpec tableSpec;
  private final HttpStoragePluginConfig config;
  private final List<SchemaPath> columns;

  @JsonCreator
  public HttpSubScan(
    @JsonProperty("config") HttpStoragePluginConfig config,
    @JsonProperty("tableSpec") HttpScanSpec tableSpec,
    @JsonProperty("columns") List<SchemaPath> columns) {
    super("user-if-needed");
    this.config = config;
    this.tableSpec = tableSpec;
    this.columns = columns;
  }
  @JsonProperty("tableSpec")
  public HttpScanSpec tableSpec() {
    return tableSpec;
  }

  @JsonProperty("columns")
  public List<SchemaPath> columns() {
    return columns;
  }

  @JsonProperty("config")
  public HttpStoragePluginConfig config() {
    return config;
  }

  @JsonIgnore
  public String getURL() {
    return tableSpec.getURL();
  }

  @JsonIgnore
  public String getFullURL() {
    String selectedConnection = tableSpec.database();
    String url = config.connections().get(selectedConnection).url();
    return url + tableSpec.tableName();
  }

 @Override
  public <T, X, E extends Throwable> T accept(
   PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    return new HttpSubScan(config, tableSpec, columns);
  }

  @Override
  @JsonIgnore
  public int getOperatorType() {
    return CoreOperatorType.HTTP_SUB_SCAN_VALUE;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return ImmutableSet.<PhysicalOperator>of().iterator();
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("tableSpec", tableSpec)
      .field("columns", columns)
      .field("config", config)
      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableSpec,columns,config);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    HttpSubScan other = (HttpSubScan) obj;
    return Objects.equals(tableSpec, other.tableSpec)
      && Objects.equals(columns, other.columns)
      && Objects.equals(config, other.config);
  }
}
