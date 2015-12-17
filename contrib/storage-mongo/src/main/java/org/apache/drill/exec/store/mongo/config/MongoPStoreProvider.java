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
package org.apache.drill.exec.store.mongo.config;

import java.io.IOException;

import org.apache.drill.exec.store.mongo.DrillMongoConstants;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.apache.drill.exec.store.sys.PStoreProvider;
import org.apache.drill.exec.store.sys.PStoreRegistry;
import org.apache.drill.exec.store.sys.local.LocalEStoreProvider;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;

public class MongoPStoreProvider implements PStoreProvider, DrillMongoConstants {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(MongoPStoreProvider.class);

  static final String pKey = "pKey";

  private MongoClient client;

  private MongoCollection<Document> collection;

  private final String mongoURL;
  private final LocalEStoreProvider localEStoreProvider;

  public MongoPStoreProvider(PStoreRegistry registry) {
    mongoURL = registry.getConfig().getString(SYS_STORE_PROVIDER_MONGO_URL);
    localEStoreProvider = new LocalEStoreProvider();
  }

  @Override
  public void start() throws IOException {
    MongoClientURI clientURI = new MongoClientURI(mongoURL);
    client = new MongoClient(clientURI);
    MongoDatabase db = client.getDatabase(clientURI.getDatabase());
    collection = db.getCollection(clientURI.getCollection()).withWriteConcern(WriteConcern.JOURNALED);
    Bson index = Indexes.ascending(pKey);
    collection.createIndex(index);
  }

  @Override
  public <V> PStore<V> getStore(PStoreConfig<V> config) throws IOException {
    switch(config.getMode()){
    case BLOB_PERSISTENT:
    case PERSISTENT:
      return new MongoPStore<>(config, collection);
    case EPHEMERAL:
      return localEStoreProvider.getStore(config);
    default:
      throw new IllegalStateException();

    }
  }

  @Override
  public void close() {
    if (client != null) {
      client.close();
    }
  }

}
