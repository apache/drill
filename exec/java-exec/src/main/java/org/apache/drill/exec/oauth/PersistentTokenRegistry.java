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
package org.apache.drill.exec.oauth;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Implementation of {@link TokenRegistry} that persists aliases tables
 * to the pre-configured persistent store.
 */
public class PersistentTokenRegistry implements TokenRegistry {
  public static final String PUBLIC_ALIASES_KEY = "$public_aliases";

  private final PersistentStore<PersistentTokenTable> store;

  public PersistentTokenRegistry(DrillbitContext context, String registryPath) {
    try {
      ObjectMapper mapper = context.getLpPersistence().getMapper().copy();
      InjectableValues injectables = new InjectableValues.Std()
        .addValue(StoreProvider.class, new StoreProvider(this::getStore));

      mapper.setInjectableValues(injectables);
      this.store = context
        .getStoreProvider()
        .getOrCreateStore(PersistentStoreConfig
          .newJacksonBuilder(mapper, PersistentTokenTable.class)
          .name(registryPath)
          .build());
    } catch (StoreException e) {
      throw new DrillRuntimeException(
        "Failure while reading and loading token table.");
    }
  }

  public PersistentStore<PersistentTokenTable> getStore() {
    return store;
  }


  @SuppressWarnings({"unchecked", "rawtypes"})
  public Iterator<Map.Entry<String, Tokens>> getAllAliases() {
    return (Iterator) store.getAll();
  }

  @Override
  public void createUserAliases(String userName) {
    if (!store.contains(userName)) {
      PersistentTokenTable aliasesTable =
        new PersistentTokenTable(new HashMap<>(), userName, new StoreProvider(this::getStore));
      store.put(userName, aliasesTable);
    }
  }

  @Override
  public void createPublicAliases() {
    if (!store.contains(PUBLIC_ALIASES_KEY)) {
      PersistentTokenTable publicAliases =
        new PersistentTokenTable(new HashMap<>(), PUBLIC_ALIASES_KEY, new StoreProvider(this::getStore));
      store.put(PUBLIC_ALIASES_KEY, publicAliases);
    }
  }

  @Override
  public void deleteUserAliases(String userName) {
    store.delete(userName);
  }

  @Override
  public void deletePublicAliases() {
    store.delete(PUBLIC_ALIASES_KEY);
  }

  @Override
  public Tokens getPublicAliases() {
    return Optional.<Tokens>ofNullable(store.get(PUBLIC_ALIASES_KEY))
      .orElse(EmptyTokens.INSTANCE);
  }

  @Override
  public void close() throws Exception {
    store.close();
  }

  public static class StoreProvider {
    private final Supplier<PersistentStore<PersistentTokenTable>> supplier;

    public StoreProvider(Supplier<PersistentStore<PersistentTokenTable>> supplier) {
      this.supplier = supplier;
    }

    public PersistentStore<PersistentTokenTable> getStore() {
      return supplier.get();
    }
  }
}
