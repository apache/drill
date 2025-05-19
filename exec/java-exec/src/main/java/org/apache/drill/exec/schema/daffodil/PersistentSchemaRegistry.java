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


import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Implementation of {@link DaffodilSchemaRegistry} that persists Daffodil schema tables
 * to the preconfigured persistent store.
 */

// TODO Start here.. Add constructor and initialization

public class PersistentSchemaRegistry implements DaffodilSchemaRegistry {
  private final PersistentStore<PersistentSchemaTable> store;

  public PersistentSchemaRegistry(DrillbitContext context, String registryPath) {
    try {
      ObjectMapper mapper = context.getLpPersistence().getMapper().copy();
      InjectableValues injectables = new InjectableValues.Std()
          .addValue(StoreProvider.class, new StoreProvider(this::getStore));

      mapper.setInjectableValues(injectables);
      this.store = context
          .getStoreProvider()
          .getOrCreateStore(PersistentStoreConfig
              .newJacksonBuilder(mapper, PersistentSchemaTable.class)
              .name(registryPath)
              .build());
    } catch (StoreException e) {
      throw new DrillRuntimeException(
          "Failure while reading and loading Daffodil schema table.");
    }
  }

  public PersistentStore<PersistentSchemaTable> getStore() {
    return store;
  }

  @Override
  public PersistentSchemaTable getSchemaTable(String name) {
    name = name.toLowerCase();
    if (!store.contains(name)) {
      createSchemaTable(name);
    }
    return store.get(name);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public Iterator<Map.Entry<String, TupleMetadata>> getAllSchemata() {
    return (Iterator) store.getAll();
  }

  @Override
  public void createSchemaTable(String pluginName) {
    // In Drill, Storage plugin names are stored in lower case. These checks make sure
    // that the tokens are associated with the correct plugin
    pluginName = pluginName.toLowerCase();
    if (!store.contains(pluginName)) {
      PersistentSchemaTable schemaTable =
          new PersistentSchemaTable(new HashMap<>(), pluginName, new StoreProvider(this::getStore));
      store.put(pluginName, schemaTable);
    }
  }

  @Override
  public void deleteSchemaTable(String pluginName) {
    pluginName = pluginName.toLowerCase();
    if (store.contains(pluginName)) {
      store.delete(pluginName);
    }
  }

  @Override
  public void close() throws Exception {
    store.close();
  }

  public static class StoreProvider {
    private final Supplier<PersistentStore<PersistentSchemaTable>> supplier;

    public StoreProvider(Supplier<PersistentStore<PersistentSchemaTable>> supplier) {
      this.supplier = supplier;
    }

    public PersistentStore<PersistentSchemaTable> getStore() {
      return supplier.get();
    }
  }
}
