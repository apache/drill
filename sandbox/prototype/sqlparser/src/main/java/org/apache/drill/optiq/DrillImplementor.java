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

import org.apache.drill.exec.ref.rse.QueueRSE.QueueOutputInfo;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.eigenbase.rel.RelNode;

/**
 * Context for converting a tree of {@link DrillRel} nodes into a Drill logical
 * plan.
 */
public class DrillImplementor {
  final ObjectMapper mapper = new ObjectMapper();
  {
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
  }
  private final ObjectNode rootNode = mapper.createObjectNode();
  private final ArrayNode operatorsNode;

  public DrillImplementor() {
    final ObjectNode headNode = mapper.createObjectNode();
    rootNode.put("head", headNode);
    headNode.put("type", "apache_drill_logical_plan");
    headNode.put("version", "1");

    final ObjectNode generatorNode = mapper.createObjectNode();
    headNode.put("generator", generatorNode);
    generatorNode.put("type", "manual");
    generatorNode.put("info", "na");

    // TODO: populate sources based on the sources of scans that occur in
    // the query
    final ArrayNode sourcesNode = mapper.createArrayNode();
    rootNode.put("storage", sourcesNode);
    
    // input file source
    {
      final ObjectNode sourceNode = mapper.createObjectNode();
      sourceNode.put("name", "donuts-json");
      sourceNode.put("type", "classpath");
      sourcesNode.add(sourceNode);
    }
    {
      final ObjectNode sourceNode = mapper.createObjectNode();
      sourceNode.put("name", "queue");
      sourceNode.put("type", "queue");
      sourcesNode.add(sourceNode);
    }
    

    
    final ArrayNode queryNode = mapper.createArrayNode();
    rootNode.put("query", queryNode);

    final ObjectNode sequenceOpNode = mapper.createObjectNode();
    queryNode.add(sequenceOpNode);
    sequenceOpNode.put("op", "sequence");

    this.operatorsNode = mapper.createArrayNode();
    sequenceOpNode.put("do", operatorsNode);
  }

  public void go(DrillRel root) {
    root.implement(this);

    // Add a last node, to write to the output queue.
    final ObjectNode writeOp = mapper.createObjectNode();
    writeOp.put("op", "store");
    writeOp.put("storageengine", "queue");
    writeOp.put("memo", "output sink");
    QueueOutputInfo output = new QueueOutputInfo();
    output.number = 0;
    writeOp.put("target", mapper.convertValue(output, JsonNode.class));
    add(writeOp);
  }

  public void add(ObjectNode operator) {
    operatorsNode.add(operator);
  }

  /** Returns the generated plan. */
  public String getJsonString() {
    String s = rootNode.toString();
    System.out.println(s);
    return s;
  }

  public void visitChild(DrillRel parent, int ordinal, RelNode child) {
    ((DrillRel) child).implement(this);
  }
}

// End DrillImplementor.java
