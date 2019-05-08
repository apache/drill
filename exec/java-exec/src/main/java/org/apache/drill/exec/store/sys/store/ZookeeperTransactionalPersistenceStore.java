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
package org.apache.drill.exec.store.sys.store;

import org.apache.curator.framework.CuratorFramework;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.serialization.InstanceSerializer;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link org.apache.drill.exec.store.sys.store.ZookeeperPersistentStore} to support transactional
 * operation for put, delete, create and get. For other single blob operation it just uses the implementation from
 * parent.
 * @param <V>
 */
public class ZookeeperTransactionalPersistenceStore <V> extends ZookeeperPersistentStore<V> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ZookeeperTransactionalPersistenceStore.class);

  public ZookeeperTransactionalPersistenceStore(final CuratorFramework framework,
                                                final PersistentStoreConfig<V> config) throws StoreException {
    super(framework, config);
  }

  public boolean putAsTransaction(Map<String, V> blobsToPut, DataChangeVersion version) {
    Map.Entry<String, V> currentEntry = null;
    boolean isSuccess = false;
    try {
      final InstanceSerializer<V> serializer = config.getSerializer();
      Map<String, byte[]> serializedBlobsToPut = new HashMap<>();
      for (Map.Entry<String, V> entry : blobsToPut.entrySet()) {
        currentEntry = entry;
        serializedBlobsToPut.put(currentEntry.getKey(), serializer.serialize(currentEntry.getValue()));
      }
      client.putAsTransaction(serializedBlobsToPut, version);
      isSuccess = true;
    } catch (IOException ex) {
      logger.error("Failed to serialize the blobs passed to write in a single transaction", ex);
    } catch (Exception ex) {
      logger.error("Failed to put the blobs in a single transaction", ex);
    }
    return isSuccess;
  }

  public boolean putAsTransaction(Map<String, V> blobsToPut) {
    return putAsTransaction(blobsToPut, null);
  }

  public boolean deleteAsTransaction(List<String> blobPathsToDelete) {
    boolean isSuccess = false;
    try {
      client.deleteAsTransaction(blobPathsToDelete);
      isSuccess = true;
    } catch (Exception ex) {
      logger.error("Failed to delete on or more blobs in a single transaction", ex);
    }
    return isSuccess;
  }

  public Map<String, V> getAllOrNone(List<String> blobsToGet, DataChangeVersion version) {
    final Map<String, V> blobsRequested = new HashMap<>();
    String currentBlob = "";
    try {
      for (String blobName : blobsToGet) {
        currentBlob = blobName;
        final V blobData = get(currentBlob, true, version);
        blobsRequested.put(currentBlob, blobData);
      }
      return blobsRequested;
    } catch (Exception ex) {
      // if here means there was error in getting one or more blob so consume the exception and just return null
      logger.error("Error while getting one or more blob in getAllOrNone call. [Details: Requested blobs: {}, Issue " +
        "Blob: {}]", String.join(",", blobsToGet), currentBlob);
    }
    return null;
  }

  public Map<String, V> getAllOrNone(List<String> blobsToGet) {
    return getAllOrNone(blobsToGet, null);
  }

  public boolean createAsTransaction(List<String> blobsToCreate) {
    boolean isSuccess = false;
    try {
      client.createAsTransaction(blobsToCreate);
      isSuccess = true;
    } catch (Exception ex) {
      logger.error("Failed to create one or more blobs in a single transaction", ex);
    }
    return isSuccess;
  }
}
