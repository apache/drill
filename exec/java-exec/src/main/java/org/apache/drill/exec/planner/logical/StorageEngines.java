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
package org.apache.drill.exec.planner.logical;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.StoragePluginConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class StorageEngines implements Iterable<Map.Entry<String, StoragePluginConfig>>{
  
  private Map<String, StoragePluginConfig> storage;
  
  @JsonCreator
  public StorageEngines(@JsonProperty("storage") Map<String, StoragePluginConfig> storage){
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
  public Iterator<Entry<String, StoragePluginConfig>> iterator() {
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
