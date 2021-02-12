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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
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
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.exec.store.security.HadoopCredentialsProvider;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.exec.store.security.UsernamePasswordCredentials;
import org.apache.drill.shaded.guava.com.google.common.cache.Cache;
import org.apache.drill.shaded.guava.com.google.common.cache.CacheBuilder;
import org.apache.drill.shaded.guava.com.google.common.cache.RemovalListener;
import org.apache.drill.shaded.guava.com.google.common.cache.RemovalNotification;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class MongoStoragePlugin extends AbstractStoragePlugin {
  private static final Logger logger = LoggerFactory.getLogger(MongoStoragePlugin.class);

  private final MongoStoragePluginConfig mongoConfig;
  private final MongoSchemaFactory schemaFactory;
  private final Cache<MongoCnxnKey, MongoClient> addressClientMap;
  private final MongoClientURI clientURI;

  public MongoStoragePlugin(
      MongoStoragePluginConfig mongoConfig,
      DrillbitContext context,
      String name) throws ExecutionSetupException {
    super(context, name);
    this.mongoConfig = mongoConfig;
    String connection = addCredentialsFromCredentialsProvider(this.mongoConfig.getConnection(), name);
    this.clientURI = new MongoClientURI(connection);
    this.addressClientMap = CacheBuilder.newBuilder()
      .expireAfterAccess(24, TimeUnit.HOURS)
      .removalListener(new AddressCloser()).build();
    this.schemaFactory = new MongoSchemaFactory(this, name);
  }

  private String addCredentialsFromCredentialsProvider(String connection, String name) {
    MongoClientURI parsed = new MongoClientURI(connection);
    if (parsed.getCredentials() == null) {
      UsernamePasswordCredentials credentials = getUsernamePasswordCredentials(name);
      try {
        // The default connection has the name "mongo" but multiple connections can be added;
        // each will need their own credentials.
        if (credentials.getUsername() != null && credentials.getPassword() != null) {
          String username = URLEncoder.encode(credentials.getUsername(), "UTF-8");
          String password = URLEncoder.encode(credentials.getPassword(), "UTF-8");
          return connection.replaceFirst("://",
              String.format("://%s:%s@", username, password));
        }
      } catch (IOException e) {
        logger.error("Error fetching mongodb username and password from configuration", e);
      }
    }
    return connection;
  }

  private UsernamePasswordCredentials getUsernamePasswordCredentials(String name) {
    CredentialsProvider credentialsProvider = mongoConfig.getCredentialsProvider();
    // for the case if empty credentials, tries to obtain credentials using HadoopCredentialsProvider
    if (credentialsProvider == null || credentialsProvider == PlainCredentialsProvider.EMPTY_CREDENTIALS_PROVIDER) {
      credentialsProvider = new HadoopCredentialsProvider(
          ImmutableMap.of(
              UsernamePasswordCredentials.USERNAME,
              DrillMongoConstants.STORE_CONFIG_PREFIX + name + DrillMongoConstants.USERNAME_CONFIG_SUFFIX,
              UsernamePasswordCredentials.PASSWORD,
              DrillMongoConstants.STORE_CONFIG_PREFIX + name + DrillMongoConstants.PASSWORD_CONFIG_SUFFIX));
    }
    return new UsernamePasswordCredentials(credentialsProvider);
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
    MongoScanSpec mongoScanSpec = selection.getListWith(new ObjectMapper(), new TypeReference<MongoScanSpec>() {
    });
    return new MongoGroupScan(userName, this, mongoScanSpec, null);
  }

  @Override
  public Set<StoragePluginOptimizerRule> getPhysicalOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
    return ImmutableSet.of(MongoPushDownFilterForScan.INSTANCE);
  }


  private static class AddressCloser implements
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
        client = new MongoClient(addresses, credential, clientURI.getOptions());
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
  public void close() {
    addressClientMap.invalidateAll();
  }

}
