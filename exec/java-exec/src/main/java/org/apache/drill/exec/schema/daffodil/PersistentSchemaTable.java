/*
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

package org.apache.drill.exec.schema.daffodil;


import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.sys.PersistentStore;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of Daffodil schema table that updates its version in persistent store after modifications.
 */
public class PersistentSchemaTable implements DaffodilSchemata {
  public final String SCHEMA_KEY = "schema";
  private final Map<String, TupleMetadata> schemata;
  private final String key;
  private final PersistentStore<org.apache.drill.exec.schema.daffodil.PersistentSchemaTable> store;

  @JsonCreator
  public PersistentSchemaTable(
      @JsonProperty("schemata") Map<String, TupleMetadata> schemata,
      @JsonProperty("key") String key,
      @JacksonInject PersistentSchemaRegistry.StoreProvider storeProvider) {
    this.schemata = schemata != null ? schemata : new HashMap<>();
    this.key = key;
    this.store = storeProvider.getStore();
  }

  @Override
  @JsonProperty("key")
  public String getKey() {
    return key;
  }

  @Override
  public TupleMetadata getSchema() {
    return null;
  }

  @Override
  public void setSchema(TupleMetadata drillSchema) {

  }

  @Override
  @JsonIgnore
  public TupleMetadata get(String schemaName) {
    return schemata.get(schemaName);
  }

  @Override
  @JsonIgnore
  public boolean put(String token, String value, boolean replace) {
    if (replace || ! schemata.containsKey(token)) {
      schemata.put(token, TupleMetadata.of(value));
      store.put(key, this);
      return true;
    }
    return false;
  }

  @Override
  @JsonIgnore
  public boolean remove(String schemaName) {
    boolean isRemoved = schemata.remove(schemaName) != null;
    store.put(key, this);
    return isRemoved;
  }

  @JsonProperty("schemata")
  public Map<String, TupleMetadata> getSchemata() {
    return schemata;
  }
}
