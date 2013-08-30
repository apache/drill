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
