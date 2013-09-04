package org.apache.drill.jdbc;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.StorageEngineConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class StorageEngines implements Iterable<Map.Entry<String, StorageEngineConfig>>{
  
  private Map<String, StorageEngineConfig> storage;
  
  @JsonCreator
  public StorageEngines(@JsonProperty("storage") Map<String, StorageEngineConfig> storage){
    this.storage = storage;
  }
  
  public static void main(String[] args) throws Exception{
    DrillConfig config = DrillConfig.create();
    String data = Resources.toString(Resources.getResource("storage-engines.json"), Charsets.UTF_8);
    StorageEngines se = config.getMapper().readValue(data,  StorageEngines.class);
    System.out.println(se);
  }

  @Override
  public String toString() {
    final int maxLen = 10;
    return "StorageEngines [storage=" + (storage != null ? toString(storage.entrySet(), maxLen) : null) + "]";
  }

  @Override
  public Iterator<Entry<String, StorageEngineConfig>> iterator() {
    return storage.entrySet().iterator();
  }

  private String toString(Collection<?> collection, int maxLen) {
    StringBuilder builder = new StringBuilder();
    builder.append("[");
    int i = 0;
    for (Iterator<?> iterator = collection.iterator(); iterator.hasNext() && i < maxLen; i++) {
      if (i > 0)
        builder.append(", ");
      builder.append(iterator.next());
    }
    builder.append("]");
    return builder.toString();
  }
  
  
}
