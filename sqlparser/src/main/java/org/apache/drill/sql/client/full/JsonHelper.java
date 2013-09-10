/**
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
package org.apache.drill.sql.client.full;

import java.util.Map;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.vector.ValueVector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonHelper.class);
  
  private ObjectMapper mapper;
  
  JsonHelper(DrillConfig config){
    mapper = config.getMapper();
  }
  
  public JsonNode get(Map<String, ValueVector> vectors, int index){
    final ObjectNode map = mapper.createObjectNode();
    for(Map.Entry<String, ValueVector> e : vectors.entrySet()){
      Object o = e.getValue().getAccessor().getObject(index);
      if(o == null) continue;
      String k = e.getKey().replace("_MAP.", "");
      
      if(o instanceof Integer) map.put(k, (Integer) o);
      else if(o instanceof Long) map.put(k, (Long) o);
      else if(o instanceof Boolean) map.put(k, (Boolean) o);
      else if(o instanceof String) map.put(k, (String) o);
      else if(o instanceof byte[]) map.put(k, new String((byte[]) o));
      else if(o instanceof Float) map.put(k, (Float) o);
      else if(o instanceof Double) map.put(k, (Double) o);
      else map.put(k, "---Unknown Type---");
      
    }
    return map; 
  }
}
