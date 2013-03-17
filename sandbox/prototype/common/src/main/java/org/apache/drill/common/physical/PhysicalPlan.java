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
package org.apache.drill.common.physical;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.PlanProperties;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.graph.Graph;
import org.apache.drill.common.graph.GraphAlgos;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.physical.pop.PhysicalOperator;
import org.apache.drill.common.physical.pop.SinkPOP;
import org.apache.drill.common.physical.pop.SourcePOP;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

@JsonPropertyOrder({ "head", "storage", "graph" })
public class PhysicalPlan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PhysicalPlan.class);
  
  Map<String, StorageEngineConfig> storageEngines;
  PlanProperties properties;
  Graph<PhysicalOperator, SinkPOP, SourcePOP> graph;
  
  @JsonCreator
  public PhysicalPlan(@JsonProperty("head") PlanProperties properties, @JsonProperty("storage") Map<String, StorageEngineConfig> storageEngines, @JsonProperty("graph") List<PhysicalOperator> operators){
    this.storageEngines = storageEngines;
    this.properties = properties;
    this.graph = Graph.newGraph(operators, SinkPOP.class, SourcePOP.class);
  }
  
  @JsonProperty("graph")
  public List<PhysicalOperator> getSortedOperators(){
    List<PhysicalOperator> list = GraphAlgos.TopoSorter.sort(graph);
    // reverse the list so that nested references are flattened rather than nested.
    return Lists.reverse(list);
  }
  
  
  @JsonProperty("storage")
  public Map<String, StorageEngineConfig> getStorageEngines() {
    return storageEngines;
  }

  @JsonProperty("head")
  public PlanProperties getProperties() {
    return properties;
  }

  /** Parses a physical plan. */
  public static PhysicalPlan parse(DrillConfig config, String planString) {
    ObjectMapper mapper = config.getMapper();
    try {
      PhysicalPlan plan = mapper.readValue(planString, PhysicalPlan.class);
      return plan;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Converts a physical plan to a string. (Opposite of {@link #parse}.) */
  public String unparse(DrillConfig config) {
    try {
      return config.getMapper().writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
  
}
