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
package org.apache.drill.exec.store.mongo;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import net.hydromatic.optiq.SchemaPlus;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.store.SchemaFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class MongoSchemaFactory implements SchemaFactory  {
  
  static final Logger logger = LoggerFactory.getLogger(MongoSchemaFactory.class);
  
  private static final String DATABASES = "databases";
  
  private final MongoClient client;
  private LoadingCache<String, List<String>> databases;
  private LoadingCache<String, List<String>> tableNameLoader;
  private LoadingCache<String, LoadingCache<String, String>> tableLoaders;
  private final String schemaName;
  private final MongoStoragePlugin schema;
  
  
  public MongoSchemaFactory(MongoStoragePlugin schema, String schemaName) throws ExecutionSetupException {
    String connection = schema.getConfig().getConnection();
    
    this.schema = schema;
    this.schemaName = schemaName;
    
    MongoClientURI clientURI = new MongoClientURI(connection);
    try {
      client = new MongoClient(clientURI);
    } catch (UnknownHostException e) {
      throw new ExecutionSetupException(e.getMessage(), e);
    }
    
    databases = CacheBuilder //
        .newBuilder() //
        .expireAfterAccess(1, TimeUnit.MINUTES) //
        .build(new DatabaseLoader());

    tableNameLoader = CacheBuilder //
        .newBuilder() //
        .expireAfterAccess(1, TimeUnit.MINUTES) //
        .build(new TableNameLoader());

    tableLoaders = CacheBuilder //
        .newBuilder() //
        .expireAfterAccess(4, TimeUnit.HOURS) //
        .maximumSize(20) //
        .build(new TableLoaderLoader());
  }
  
  private class DatabaseLoader extends CacheLoader<String, List<String>> {

    @Override
    public List<String> load(String key) throws Exception {
      if (!DATABASES.equals(key))
        throw new UnsupportedOperationException();
        return client.getDatabaseNames();
    }
    
  }
  
  private class TableNameLoader extends CacheLoader<String, List<String>> {

    @Override
    public List<String> load(String dbName) throws Exception {
        DB db = client.getDB(dbName);
        Set<String> collectionNames = db.getCollectionNames();
        return new ArrayList<>(collectionNames);
    }

  }
  
  private class TableLoaderLoader extends CacheLoader<String, LoadingCache<String, String>> {

    @Override
    public LoadingCache<String, String> load(String key) throws Exception {
      return CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.MINUTES).build(new TableLoader(key));
    }

  }
  
  private class TableLoader extends CacheLoader<String, String> {

    private final String dbName;

    public TableLoader(String dbName) {
      super();
      this.dbName = dbName;
    }

    @Override
    public String load(String key) throws Exception {
      DB db = client.getDB(dbName);
      return db.getCollection(key).getName();
    }
  }

  @Override
  public void registerSchemas(UserSession session, SchemaPlus parent) {
    
  }

}
