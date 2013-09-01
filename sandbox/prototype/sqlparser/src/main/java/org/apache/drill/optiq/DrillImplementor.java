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
package org.apache.drill.optiq;

import java.io.IOException;
import java.util.Set;

import org.apache.drill.exec.ref.rse.QueueRSE.QueueOutputInfo;
import org.apache.drill.jdbc.DrillTable;
import org.eigenbase.rel.RelNode;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;

/**
 * Context for converting a tree of {@link DrillRel} nodes into a Drill logical plan.
 */
public class DrillImplementor {
  
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillImplementor.class);
  
  final ObjectMapper mapper = new ObjectMapper();
  {
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
  }
  private final ObjectNode rootNode = mapper.createObjectNode();
  private final ArrayNode operatorsNode;
  private final ObjectNode sourcesNode;
  private Set<DrillTable> tables = Sets.newHashSet();
  private final boolean isRef;
  
  public DrillImplementor(boolean isRef) {
    this.isRef = isRef;
    final ObjectNode headNode = mapper.createObjectNode();
    rootNode.put("head", headNode);
    headNode.put("type", "APACHE_DRILL_LOGICAL");
    headNode.put("version", "1");

    final ObjectNode generatorNode = mapper.createObjectNode();
    headNode.put("generator", generatorNode);
    generatorNode.put("type", "optiq");
    generatorNode.put("info", "na");

    // TODO: populate sources based on the sources of scans that occur in
    // the query
    sourcesNode = mapper.createObjectNode();
    rootNode.put("storage", sourcesNode);

    if(isRef){
      {
        final ObjectNode sourceNode = mapper.createObjectNode();
        sourceNode.put("type", "classpath");
        sourcesNode.put("donuts-json", sourceNode);
      }
      {
        final ObjectNode sourceNode = mapper.createObjectNode();
        sourceNode.put("type", "queue");
        sourcesNode.put("queue", sourceNode);
      }
    }


    final ArrayNode queryNode = mapper.createArrayNode();
    rootNode.put("query", queryNode);

    this.operatorsNode = queryNode;
  }
  
  public void registerSource(DrillTable table){
    if(tables.add(table)){
      sourcesNode.put(table.getStorageEngineName(), mapper.convertValue(table.getStorageEngineConfig(), JsonNode.class));  
    }
  }

  public int go(DrillRel root) {
    int inputId = root.implement(this);

    // Add a last node, to write to the output queue.
    final ObjectNode writeOp = mapper.createObjectNode();
    writeOp.put("op", "store");
    writeOp.put("input", inputId);
    writeOp.put("storageengine", "queue");
    writeOp.put("memo", "output sink");
    QueueOutputInfo output = new QueueOutputInfo();
    output.number = 0;
    writeOp.put("target", mapper.convertValue(output, JsonNode.class));
    return add(writeOp);
  }

  public int add(ObjectNode operator) {
    operatorsNode.add(operator);
    final int id = operatorsNode.size();
    operator.put("@id", id);
    return id;
  }

  /** Returns the generated plan. */
  public String getJsonString() {
    String s = rootNode.toString();
    
    if(logger.isDebugEnabled()){
      JsonNode node;
      try {
        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        node = mapper.readValue(s, JsonNode.class);
        logger.debug("Optiq Generated Logical Plan: {}", mapper.writeValueAsString(node));
      } catch (IOException e) {
        logger.error("Failure while trying to parse logical plan string of {}", s, e);
      }
      
    }
    return s;
  }

  public int visitChild(DrillRel parent, int ordinal, RelNode child) {
    ((DrillRel) child).implement(this);
    return operatorsNode.size();
  }
}
