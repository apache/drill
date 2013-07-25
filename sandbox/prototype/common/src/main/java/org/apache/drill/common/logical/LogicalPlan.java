/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.common.logical;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.PlanProperties;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.graph.Graph;
import org.apache.drill.common.graph.GraphAlgos;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.SinkOperator;
import org.apache.drill.common.logical.data.SourceOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

@JsonPropertyOrder({ "head", "storage", "query" })
public class LogicalPlan {
  static final Logger logger = LoggerFactory.getLogger(LogicalPlan.class);

  private final PlanProperties properties;
  private final Map<String, StorageEngineConfig> storageEngineMap;
  private final Graph<LogicalOperator, SinkOperator, SourceOperator> graph;

  @JsonCreator
  public LogicalPlan(@JsonProperty("head") PlanProperties head,
      @JsonProperty("storage") Map<String, StorageEngineConfig> storageEngineMap,
      @JsonProperty("query") List<LogicalOperator> operators) {
    this.storageEngineMap = storageEngineMap;
    this.properties = head;
    this.graph = Graph.newGraph(operators, SinkOperator.class, SourceOperator.class);
  }

  @JsonProperty("query")
  public List<LogicalOperator> getSortedOperators() {
    return GraphAlgos.TopoSorter.sortLogical(graph);
  }

  public StorageEngineConfig getStorageEngine(String name) {
    return storageEngineMap.get(name);
  }

  @JsonIgnore
  public Graph<LogicalOperator, SinkOperator, SourceOperator> getGraph() {
    return graph;
  }

  @JsonProperty("head")
  public PlanProperties getProperties() {
    return properties;
  }

  @JsonProperty("storage")
  public Map<String, StorageEngineConfig> getStorageEngines() {
    return storageEngineMap;
  }

  public String toJsonString(DrillConfig config) throws JsonProcessingException {
    return config.getMapper().writeValueAsString(this);
  }

  /** Parses a logical plan. */
  public static LogicalPlan parse(DrillConfig config, String planString) {
    ObjectMapper mapper = config.getMapper();
    try {
      LogicalPlan plan = mapper.readValue(planString, LogicalPlan.class);
      return plan;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Converts a logical plan to a string. (Opposite of {@link #parse}.) */
  public String unparse(DrillConfig config) {
    try {
      return config.getMapper().writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
