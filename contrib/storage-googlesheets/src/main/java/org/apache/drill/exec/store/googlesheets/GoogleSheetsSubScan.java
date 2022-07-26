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

package org.apache.drill.exec.store.googlesheets;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractSubScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.base.filter.ExprNode;
import org.apache.drill.exec.store.base.filter.ExprNode.ColRelOpConstNode;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonTypeName("googlesheets-sub-scan")
public class GoogleSheetsSubScan extends AbstractSubScan {

  public static final String OPERATOR_TYPE = "GOOGLESHEETS_SUB_SCAN";

  private final GoogleSheetsStoragePluginConfig config;
  private final GoogleSheetsScanSpec scanSpec;
  private final List<SchemaPath> columns;
  private final Map<String, ColRelOpConstNode> filters;
  private final int maxRecords;
  private final TupleMetadata schema;

  @JsonCreator
  public GoogleSheetsSubScan(
    @JsonProperty("userName") String username,
    @JsonProperty("config") GoogleSheetsStoragePluginConfig config,
    @JsonProperty("tableSpec") GoogleSheetsScanSpec scanSpec,
    @JsonProperty("columns") List<SchemaPath> columns,
    @JsonProperty("filters") Map<String, ColRelOpConstNode> filters,
    @JsonProperty("maxRecords") int maxRecords,
    @JsonProperty("schema") TupleMetadata schema) {
    super(username);
    this.config = config;
    this.scanSpec = scanSpec;
    this.columns = columns;
    this.filters = filters;
    this.schema = schema;
    this.maxRecords = maxRecords;
  }

  @JsonProperty("config")
  public GoogleSheetsStoragePluginConfig getConfig() {
    return config;
  }

  @JsonProperty("tableSpec")
  public GoogleSheetsScanSpec getScanSpec() {
    return scanSpec;
  }

  @JsonProperty("columns")
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty("filters")
  public Map<String, ExprNode.ColRelOpConstNode> getFilters() {
    return filters;
  }

  @JsonProperty("maxRecords")
  public int getMaxRecords() {
    return maxRecords;
  }

  @JsonProperty("schema")
  public TupleMetadata getSchema() {
    return schema;
  }

  @Override
  public <T, X, E extends Throwable> T accept(
    PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return ImmutableSet.<PhysicalOperator>of().iterator();
  }

  @Override
  @JsonIgnore
  public String getOperatorType() {
    return OPERATOR_TYPE;
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("config", config)
      .field("tableSpec", scanSpec)
      .field("columns", columns)
      .field("filters", filters)
      .field("maxRecords", maxRecords)
      .field("schema", schema)
      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(config, scanSpec, columns, filters, maxRecords, schema);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    GoogleSheetsSubScan other = (GoogleSheetsSubScan) obj;
    return Objects.equals(scanSpec, other.scanSpec)
      && Objects.equals(config, other.config)
      && Objects.equals(columns, other.columns)
      && Objects.equals(filters, other.filters)
      && Objects.equals(schema, other.schema)
      && Objects.equals(maxRecords, other.maxRecords);
  }
}
