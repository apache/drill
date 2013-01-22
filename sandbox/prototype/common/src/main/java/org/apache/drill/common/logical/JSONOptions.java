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

import java.io.IOException;

import org.apache.drill.common.logical.JSONOptions.De;
import org.apache.drill.common.logical.JSONOptions.Se;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

@JsonSerialize(using = Se.class)
@JsonDeserialize(using = De.class)
public class JSONOptions {

  final static Logger logger = LoggerFactory.getLogger(JSONOptions.class);
  
  private TreeNode root;
  
  private JSONOptions(TreeNode n){
    this.root = n;
  }
  
  public static class De extends StdDeserializer<JSONOptions> {
    
    public De() {
      super(JSONOptions.class);
      logger.debug("Creating Deserializer.");
    }

    @Override
    public JSONOptions deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException,
        JsonProcessingException {
      //logger.debug("Reading tree.");
      return new JSONOptions( (TreeNode) jp.readValueAsTree());
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
