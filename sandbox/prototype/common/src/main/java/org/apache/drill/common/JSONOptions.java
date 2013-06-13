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
package org.apache.drill.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.JSONOptions.De;
import org.apache.drill.common.JSONOptions.Se;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.LogicalPlanParsingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

@JsonSerialize(using = Se.class)
@JsonDeserialize(using = De.class)
public class JSONOptions {
  
  final static Logger logger = LoggerFactory.getLogger(JSONOptions.class);
  
  private JsonNode root;
  private JsonLocation location;
  
  private JSONOptions(JsonNode n, JsonLocation location){
    this.root = n;
    this.location = location;
  }
  
  public <T> T getWith(DrillConfig config, Class<T> c){
    try {
      //logger.debug("Read tree {}", root);
      return config.getMapper().treeToValue(root, c);
    } catch (JsonProcessingException e) {
      throw new LogicalPlanParsingException(String.format("Failure while trying to convert late bound json options to type of %s. Reference was originally located at line %d, column %d.", c.getCanonicalName(), location.getLineNr(), location.getColumnNr()), e);
    }
  }

  public <T> T getListWith(DrillConfig config, TypeReference<T> t) throws IOException {
      ObjectMapper mapper = config.getMapper();
      return mapper.treeAsTokens(root).readValueAs(t);
     // return mapper.treeToValue(root,  mapper.getTypeFactory().constructCollectionType(List.class, c));
  }
  
  public JsonNode path(String name){
    return root.path(name);
  }
  
  public static class De extends StdDeserializer<JSONOptions> {
    
    public De() {
      super(JSONOptions.class);
      logger.debug("Creating Deserializer.");
    }

    @Override
    public JSONOptions deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException,
        JsonProcessingException {
      JsonLocation l = jp.getTokenLocation();
//      logger.debug("Reading tree.");
      TreeNode n = jp.readValueAsTree();
//      logger.debug("Tree {}", n);
      if(n instanceof JsonNode){
        return new JSONOptions( (JsonNode) n, l); 
      }else{
        throw new IllegalArgumentException(String.format("Received something other than a JsonNode %s", n));
      }
    }

  }

  public static class Se extends StdSerializer<JSONOptions> {

    public Se() {
      super(JSONOptions.class);
    }

    @Override
    public void serialize(JSONOptions value, JsonGenerator jgen, SerializerProvider provider) throws IOException,
        JsonGenerationException {
      jgen.writeTree(value.root);
    }

  }
}
