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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;

public class MongoCnxnManager {

  private static final Logger logger = LoggerFactory
      .getLogger(MongoCnxnManager.class);
  private static Cache<ServerAddress, MongoClient> addressClientMap;

  static {
    addressClientMap = CacheBuilder.newBuilder().maximumSize(5)
        .expireAfterAccess(10, TimeUnit.MINUTES)
        .removalListener(new AddressCloser()).build();
  }

  private static class AddressCloser implements
      RemovalListener<ServerAddress, MongoClient> {
    @Override
    public synchronized void onRemoval(
        RemovalNotification<ServerAddress, MongoClient> removal) {
      removal.getValue().close();
      ;
      logger.debug("Closed connection to {}.", removal.getKey().toString());
    }
  }

  public synchronized static MongoClient getClient(
      List<ServerAddress> addresses, MongoClientOptions clientOptions)
      throws UnknownHostException {
    // Take the first replica from the replicated servers
    ServerAddress serverAddress = addresses.get(0);
    MongoClient client = addressClientMap.getIfPresent(serverAddress);
    if (client == null) {
      client = new MongoClient(addresses, clientOptions);
      addressClientMap.put(serverAddress, client);
      logger.debug("Created connection to {}.", serverAddress.toString());
      logger.debug("Number of connections opened are {}.",
          addressClientMap.size());
    }
    return client;
  }
}
