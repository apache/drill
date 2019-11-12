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
package org.apache.drill.exec.metastore.analyze;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.NamedExpression;

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Class which provides information required for producing metadata aggregation when performing analyze.
 */
@JsonDeserialize(builder = MetadataAggregateContext.MetadataAggregateContextBuilder.class)
public class MetadataAggregateContext {
  private final List<NamedExpression> groupByExpressions;
  private final List<SchemaPath> interestingColumns;
  private final List<SchemaPath> excludedColumns;
  private final boolean createNewAggregations;

  public MetadataAggregateContext(MetadataAggregateContextBuilder builder) {
    this.groupByExpressions = builder.groupByExpressions;
    this.interestingColumns = builder.interestingColumns;
    this.createNewAggregations = builder.createNewAggregations;
    this.excludedColumns = builder.excludedColumns;
  }

  @JsonProperty
  public List<NamedExpression> groupByExpressions() {
    return groupByExpressions;
  }

  @JsonProperty
  public List<SchemaPath> interestingColumns() {
    return interestingColumns;
  }

  @JsonProperty
  public boolean createNewAggregations() {
    return createNewAggregations;
  }

  @JsonProperty
  public List<SchemaPath> excludedColumns() {
    return excludedColumns;
  }

  @Override
  public String toString() {
    return new StringJoiner(",\n", MetadataAggregateContext.class.getSimpleName() + "[", "]")
        .add("groupByExpressions=" + groupByExpressions)
        .add("interestingColumns=" + interestingColumns)
        .add("createNewAggregations=" + createNewAggregations)
        .add("excludedColumns=" + excludedColumns)
        .toString();
  }

  public static MetadataAggregateContextBuilder builder() {
    return new MetadataAggregateContextBuilder();
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class MetadataAggregateContextBuilder {
    private List<NamedExpression> groupByExpressions;
    private List<SchemaPath> interestingColumns;
    private Boolean createNewAggregations;
    private List<SchemaPath> excludedColumns;

    public MetadataAggregateContextBuilder groupByExpressions(List<NamedExpression> groupByExpressions) {
      this.groupByExpressions = groupByExpressions;
      return this;
    }

    public MetadataAggregateContextBuilder interestingColumns(List<SchemaPath> interestingColumns) {
      this.interestingColumns = interestingColumns;
      return this;
    }

    public MetadataAggregateContextBuilder createNewAggregations(boolean createNewAggregations) {
      this.createNewAggregations = createNewAggregations;
      return this;
    }

    public MetadataAggregateContextBuilder excludedColumns(List<SchemaPath> excludedColumns) {
      this.excludedColumns = excludedColumns;
      return this;
    }

    public MetadataAggregateContext build() {
      Objects.requireNonNull(groupByExpressions, "groupByExpressions were not set");
      Objects.requireNonNull(createNewAggregations, "createNewAggregations was not set");
      Objects.requireNonNull(excludedColumns, "excludedColumns were not set");
      return new MetadataAggregateContext(this);
    }
  }
}
