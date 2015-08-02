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
package org.apache.drill.exec.store.mpjdbc;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import org.apache.drill.exec.store.mpjdbc.MPJdbcClientOptions;

public class MPJdbcCnxnManager {

  private static final Logger logger = LoggerFactory
      .getLogger(MPJdbcCnxnManager.class);
  private static Cache<String, MPJdbcClient> uriClientMap;

  static {
    uriClientMap = CacheBuilder.newBuilder().maximumSize(5)
        .expireAfterAccess(10, TimeUnit.MINUTES)
        .removalListener(new uriCloser()).build();
  }

  private static class uriCloser implements RemovalListener<String, MPJdbcClient> {

    @Override
    public synchronized void onRemoval(
        RemovalNotification<String, MPJdbcClient> removal) {
      removal.getValue().close();
      logger.debug("Closed connection to {}.", removal.getKey().toString());
    }

  }

  public synchronized static MPJdbcClient getClient(String uri,
      MPJdbcClientOptions clientOptions, MPJdbcFormatPlugin plugin) {
    MPJdbcClient client = uriClientMap.getIfPresent(uri);
    if (client == null) {
      client = new MPJdbcClient(uri, clientOptions,plugin);
      if (client.getConnection() != null) {
        uriClientMap.put(uri, client);
      } else {
        return null;
      }
    }

    return client;
  }
}
