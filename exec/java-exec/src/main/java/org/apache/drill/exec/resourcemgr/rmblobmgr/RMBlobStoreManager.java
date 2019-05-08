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
package org.apache.drill.exec.resourcemgr.rmblobmgr;

import org.apache.drill.exec.resourcemgr.NodeResources;
import org.apache.drill.exec.resourcemgr.config.QueryQueueConfig;

import java.util.Map;

/**
 * Interface that defines implementation of StoreManager for RMStateBlobs. BlobManager is the responsible for
 * initialization of blobs and all the reads and writes to the blobs. It exposes mainly 2 apis which are used when blobs
 * needs to be updated. One is to update the blobs to reflect that resources are reserved for this query whereas
 * other is used to update the blobs to free up the resources. There can be different implementations of the store
 * manager where one can manage the blobs lazily whereas other will manage it in strongly consistent manner. Current
 * implementation {@link RMConsistentBlobStoreManager} is using the later approach.
 */
public interface RMBlobStoreManager {
  void reserveResources(Map<String, NodeResources> queryResourceAssignment, QueryQueueConfig selectedQueue,
                        String leaderId, String queryId, String foremanUUID) throws Exception;

  String freeResources(Map<String, NodeResources> queryResourceAssignment, QueryQueueConfig selectedQueue,
                     String leaderId, String queryId, String foremanUUID) throws Exception;

  void registerResource(String selfUUID, NodeResources resourceToRegister) throws Exception;

  void updateLeadershipInformation(String queueName, String leaderUUID) throws Exception;
}
