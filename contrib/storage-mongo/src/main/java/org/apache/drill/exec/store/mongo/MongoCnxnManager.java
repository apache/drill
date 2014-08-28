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
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;

public class MongoCnxnManager {

  private static Cache<ServerAddress, MongoClient> addressClientMap;
  
  private MongoCnxnManager() {
    addressClientMap = CacheBuilder.newBuilder()
                                   .maximumSize(10)
                                   .expireAfterAccess(5, TimeUnit.MINUTES)
                                   .removalListener(new AddressCloser())
                                   .build();
  }

  private class AddressCloser implements RemovalListener<ServerAddress, MongoClient> {
    @Override
    public void onRemoval(RemovalNotification<ServerAddress, MongoClient> notification) {
      notification.getValue().close();
    }
  }

  public static MongoClient getClient(List<ServerAddress> addresses, MongoClientOptions clientOptions) throws UnknownHostException {
    // Take the first replica from the replicated servers
    ServerAddress serverAddress = addresses.get(0);
    MongoClient mongoClient = addressClientMap.getIfPresent(serverAddress);
    if (mongoClient == null) {
      mongoClient = new MongoClient(addresses, clientOptions);
      addressClientMap.put(serverAddress, mongoClient);
    }
    return mongoClient;
  }
}
