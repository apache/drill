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
package org.apache.drill.exec.store.druid.druid;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

@JsonPropertyOrder({ "queryType", "dataSource", "columns", "filter", "intervals", "batchSize" })
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ScanQuery {

    @JsonProperty
  private static final String queryType = "scan";

  private final String dataSource;

  private final Integer batchSize;

  private final List<String> columns;
  private final ObjectNode filter;
  private final List<String> intervals;

  public ScanQuery(@JsonProperty("dataSource") String dataSource,
                   @JsonProperty("dimensions") List<String> columns,
                   @JsonProperty("filter") ObjectNode filter,
                   @JsonProperty("intervals") List<String> intervals,
                   @JsonProperty("batchSize") Integer batchSize) {
    this.dataSource = dataSource;
    this.batchSize = batchSize;
    this.columns = columns;
    this.filter = filter;
    this.intervals = intervals;
  }

  public String getQueryType() {
    return queryType;
  }

  public String getDataSource() {
    return dataSource;
  }

  public List<String> getColumns() {
    return columns;
  }

  public List<String> getIntervals() {
    return intervals;
  }

  public ObjectNode getFilter() {
    return filter;
  }

  public Integer getBatchSize() {
    return batchSize;
  }
}
