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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.LogicalPlanParsingException;
import org.apache.drill.common.logical.OperatorGraph.OpNode;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.graph.GraphAlgos;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonPropertyOrder({"head", "storage", "query"})
public class LogicalPlan {

  private final PlanProperties properties;
  private final Map<String, StorageEngineConfig> storageEngines;
  private final List<LogicalOperator> operators;
  private final OperatorGraph graph;

  @JsonCreator
  private LogicalPlan(@JsonProperty("head") PlanProperties head,
                      @JsonProperty("storage") List<StorageEngineConfig> storageEngines,
                      @JsonProperty("query") List<LogicalOperator> operators) {
    if (storageEngines == null)
      storageEngines = ImmutableList.of();
    this.properties = head;
    this.storageEngines = new HashMap<>(storageEngines.size());
    this.operators = operators;
    for (StorageEngineConfig store : storageEngines) {
      StorageEngineConfig old = this.storageEngines.put(store.getName(), store);
      if (old != null)
        throw new LogicalPlanParsingException(String.format("Each storage engine must have a unique name. " +
          "You provided more than one data source with the same name of '%s'", store.getName()));
    }

    this.graph = new OperatorGraph(operators);
  }

  @JsonProperty("query")
  public List<LogicalOperator> getSortedOperators() {
    List<OpNode> nodes = GraphAlgos.TopoSorter.sort(graph.getAdjList());
    Iterable<LogicalOperator> i = Iterables.transform(nodes, new Function<OpNode, LogicalOperator>() {
      public LogicalOperator apply(OpNode o) {
        return o.getNodeValue();
      }
    });
    return Lists.newArrayList(i);
  }

  public StorageEngineConfig getStorageEngine(String name) {
    StorageEngineConfig ds = storageEngines.get(name);
    if (ds == null) throw new LogicalPlanParsingException(String.format("Unknown data source named [%s].", name));
    return ds;
  }

  @JsonIgnore
  public OperatorGraph getGraph() {
    return graph;
  }

  @JsonProperty("head")
  public PlanProperties getProperties() {
    return properties;
  }


  @JsonProperty("storage")
  public List<StorageEngineConfig> getStorageEngines() {
    return new ArrayList<>(storageEngines.values());
  }

  public String toJsonString(DrillConfig config) throws JsonProcessingException {
    return config.getMapper().writeValueAsString(this);
  }


  public static void main(String[] args) throws Exception {
    DrillConfig config = DrillConfig.create();
    String externalPlan = Files.toString(new File("src/test/resources/simple_plan.json"), Charsets.UTF_8);
    LogicalPlan plan = parse(config, externalPlan);
  }

  /**
   * Parses a logical plan.
   */
  public static LogicalPlan parse(DrillConfig config, String planString) {
    ObjectMapper mapper = config.getMapper();
    try {
      LogicalPlan plan = mapper.readValue(planString, LogicalPlan.class);
      return plan;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Converts a logical plan to a string. (Opposite of {@link #parse}.)
   */
  public String unparse(DrillConfig config) {
    try {
      return config.getMapper().writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static LogicalPlanBuilder builder() {
    return new LogicalPlanBuilder();
  }

  public LogicalPlanBuilder toBuilder() {
    return new LogicalPlanBuilder()
      .planProperties(getProperties())
      .logicalOperators(operators)
      .storageEngines(storageEngines.values());
  }

  public static class LogicalPlanBuilder {

    private PlanProperties planProperties;
    private ImmutableList.Builder<StorageEngineConfig> storageEngines = ImmutableList.builder();
    private ImmutableList.Builder<LogicalOperator> operators = ImmutableList.builder();

    public LogicalPlanBuilder planProperties(PlanProperties planProperties) {
      this.planProperties = planProperties;
      return this;
    }

    public LogicalPlanBuilder addStorageEngine(StorageEngineConfig config) {
      this.storageEngines.add(config);
      return this;
    }

    public LogicalPlanBuilder storageEngines(Iterable<StorageEngineConfig> storageEngines) {
      this.storageEngines = ImmutableList.builder();
      this.storageEngines.addAll(storageEngines);
      return this;
    }

    public LogicalPlanBuilder addLogicalOperator(LogicalOperator operator) {
      this.operators.add(operator);
      return this;
    }

    public LogicalPlanBuilder logicalOperators(List<LogicalOperator> operators) {
      this.operators = ImmutableList.builder();
      this.operators.addAll(operators);
      return this;
    }

    public LogicalPlan build() {
      return new LogicalPlan(this.planProperties, this.storageEngines.build(), this.operators.build());
    }
  }
}
