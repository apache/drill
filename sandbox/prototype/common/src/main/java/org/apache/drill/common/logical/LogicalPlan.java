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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.drill.common.logical.OperatorGraph.OpNode;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.graph.GraphAlgos;
import org.apache.drill.common.logical.sources.DataSource;
import org.apache.drill.common.logical.sources.record.RecordMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

@JsonPropertyOrder({"head", "sources", "query"})
public class LogicalPlan {
  private static final Logger logger = LoggerFactory.getLogger(LogicalPlan.class);
	private final PlanProperties properties;
	private final Map<String, DataSource> dataSources;
	private final OperatorGraph graph;
	
	@JsonCreator
	public LogicalPlan(@JsonProperty("head") PlanProperties head, @JsonProperty("sources") List<DataSource> sources, @JsonProperty("query") List<LogicalOperator> operators){
	  this.properties = head;
	  this.dataSources = new HashMap<String, DataSource>(sources.size());
    for(DataSource ds: sources){
      DataSource old = dataSources.put(ds.getName(), ds);
      if(old != null) throw new IllegalArgumentException("Each data source must have a unique name.  You provided more than one data source with the same name of '" + ds.getName() + "'");
    }
    
    this.graph = new OperatorGraph(operators);
	}
	
	@JsonProperty("query")
	public List<LogicalOperator> getSortedOperators(){
	  List<OpNode> nodes = GraphAlgos.TopoSorter.sort(graph.getAdjList());
	  Iterable<LogicalOperator> i = Iterables.transform(nodes, new Function<OpNode, LogicalOperator>(){
	    public LogicalOperator apply(OpNode o){
	      return o.getNodeValue();
	    }
	  });
	  return Lists.newArrayList(i);
	}

	public DataSource getDataSource(String name){
	  DataSource ds = dataSources.get(name);
	  if(ds == null) throw new IllegalArgumentException(String.format("Unknown data source named [%s].", name));
	  return ds;
	}
	
	@JsonIgnore
	public OperatorGraph getGraph(){
	  return graph;
	}
	
	@JsonProperty("head")
  public PlanProperties getProperties() {
    return properties;
  }


	@JsonProperty("sources") 
  public List<DataSource> getDataSources() {
    return new ArrayList<DataSource>(dataSources.values());
  }

  private static ObjectMapper createMapper() {
    ObjectMapper mapper = new ObjectMapper();

    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    mapper.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(Feature.ALLOW_COMMENTS, true);

    mapper.registerSubtypes(LogicalOperator.SUB_TYPES);
    mapper.registerSubtypes(DataSource.SUB_TYPES);
    mapper.registerSubtypes(RecordMaker.SUB_TYPES);
    return mapper;
  }

  public static void main(String[] args) throws Exception {
    String externalPlan = Files.toString(new File("src/test/resources/simple_plan.json"), Charsets.UTF_8);
    LogicalPlan plan = parse(externalPlan);
  }

  /** Parses a logical plan. */
  public static LogicalPlan parse(String planString) {
    ObjectMapper mapper = createMapper();
    try {
      LogicalPlan plan = mapper.readValue(planString, LogicalPlan.class);
      System.out.println(mapper.writeValueAsString(plan));
      return plan;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Converts a logical plan to a string. (Opposite of {@link #parse}.) */
  public String unparse() {
    try {
      return createMapper().writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
