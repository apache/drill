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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.LogicalPlanPersistence;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.persistence.ScanResult;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class StoragePlugins implements Iterable<Map.Entry<String, StoragePluginConfig>>{

  private Map<String, StoragePluginConfig> storage;

  @JsonCreator
  public StoragePlugins(@JsonProperty("storage") Map<String, StoragePluginConfig> storage) {
    this.storage = storage;
  }

  public static void main(String[] args) throws Exception{
    DrillConfig config = DrillConfig.create();
    ScanResult scanResult = ClassPathScanner.fromPrescan(config);
    LogicalPlanPersistence lpp = new LogicalPlanPersistence(config, scanResult);
    String data = Resources.toString(Resources.getResource("storage-engines.json"), Charsets.UTF_8);
    StoragePlugins se = lpp.getMapper().readValue(data,  StoragePlugins.class);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    lpp.getMapper().writeValue(System.out, se);
    lpp.getMapper().writeValue(os, se);
    se = lpp.getMapper().readValue(new ByteArrayInputStream(os.toByteArray()), StoragePlugins.class);
    System.out.println(se);
  }

  @JsonProperty("storage")
  public Map<String, StoragePluginConfig> getStorage() {
    return storage;
  }

  @Override
  public String toString() {
    final int maxLen = 10;
    return "StoragePlugins [storage=" + (storage != null ? toString(storage.entrySet(), maxLen) : null) + "]";
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
      if (i > 0) {
        builder.append(", ");
      }
      builder.append(iterator.next());
    }
    builder.append("]");
    return builder.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof StoragePlugins)) {
      return false;
    }
    return storage.equals(((StoragePlugins) obj).getStorage());
  }

}
