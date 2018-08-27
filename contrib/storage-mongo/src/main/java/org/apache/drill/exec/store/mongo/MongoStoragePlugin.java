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
package org.apache.drill.exec.store.mongo;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.mongo.schema.MongoSchemaFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.shaded.guava.com.google.common.cache.Cache;
import org.apache.drill.shaded.guava.com.google.common.cache.CacheBuilder;
import org.apache.drill.shaded.guava.com.google.common.cache.RemovalListener;
import org.apache.drill.shaded.guava.com.google.common.cache.RemovalNotification;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

public class MongoStoragePlugin extends AbstractStoragePlugin {
  static final Logger logger = LoggerFactory
      .getLogger(MongoStoragePlugin.class);

  private final MongoStoragePluginConfig mongoConfig;
  private final MongoSchemaFactory schemaFactory;
  private final Cache<MongoCnxnKey, MongoClient> addressClientMap;
  private final MongoClientURI clientURI;

  public MongoStoragePlugin(MongoStoragePluginConfig mongoConfig,
      DrillbitContext context, String name) throws IOException,
      ExecutionSetupException {
    super(context, name);
    this.mongoConfig = mongoConfig;
    this.clientURI = new MongoClientURI(this.mongoConfig.getConnection());
    this.addressClientMap = CacheBuilder.newBuilder()
        .expireAfterAccess(24, TimeUnit.HOURS)
        .removalListener(new AddressCloser()).build();
    this.schemaFactory = new MongoSchemaFactory(this, name);
  }

  @Override
  public MongoStoragePluginConfig getConfig() {
    return mongoConfig;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
    MongoScanSpec mongoScanSpec = selection.getListWith(new ObjectMapper(), new TypeReference<MongoScanSpec>() {});
    return new MongoGroupScan(userName, this, mongoScanSpec, null);
  }

  @Override
  public Set<StoragePluginOptimizerRule> getPhysicalOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
    return ImmutableSet.of(MongoPushDownFilterForScan.INSTANCE);
  }


  private class AddressCloser implements
      RemovalListener<MongoCnxnKey, MongoClient> {
    @Override
    public synchronized void onRemoval(
        RemovalNotification<MongoCnxnKey, MongoClient> removal) {
      removal.getValue().close();
      logger.debug("Closed connection to {}.", removal.getKey().toString());
    }
  }

  public MongoClient getClient(String host) {
    return getClient(Collections.singletonList(new ServerAddress(host)));
  }

  public MongoClient getClient() {
    List<String> hosts = clientURI.getHosts();
    List<ServerAddress> addresses = Lists.newArrayList();
    for (String host : hosts) {
      addresses.add(new ServerAddress(host));
    }
    return getClient(addresses);
  }

  public synchronized MongoClient getClient(List<ServerAddress> addresses) {
    // Take the first replica from the replicated servers
    final ServerAddress serverAddress = addresses.get(0);
    final MongoCredential credential = clientURI.getCredentials();
    String userName = credential == null ? null : credential.getUserName();
    MongoCnxnKey key = new MongoCnxnKey(serverAddress, userName);
    MongoClient client = addressClientMap.getIfPresent(key);
    if (client == null) {
      if (credential != null) {
        List<MongoCredential> credentialList = Arrays.asList(credential);
        client = new MongoClient(addresses, credentialList, clientURI.getOptions());
      } else {
        client = new MongoClient(addresses, clientURI.getOptions());
      }
      addressClientMap.put(key, client);
      logger.debug("Created connection to {}.", key.toString());
      logger.debug("Number of open connections {}.", addressClientMap.size());
    }
    return client;
  }

  @Override
  public void close() throws Exception {
    addressClientMap.invalidateAll();
  }

}
