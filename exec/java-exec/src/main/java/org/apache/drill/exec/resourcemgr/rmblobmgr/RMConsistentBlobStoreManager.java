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

import avro.shaded.com.google.common.annotations.VisibleForTesting;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.coord.zk.ZKClusterCoordinator;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.resourcemgr.NodeResources;
import org.apache.drill.exec.resourcemgr.NodeResources.NodeResourcesDe;
import org.apache.drill.exec.resourcemgr.config.QueryQueueConfig;
import org.apache.drill.exec.resourcemgr.rmblobmgr.exception.LeaderChangeException;
import org.apache.drill.exec.resourcemgr.rmblobmgr.exception.RMBlobUpdateException;
import org.apache.drill.exec.resourcemgr.rmblobmgr.exception.ResourceUnavailableException;
import org.apache.drill.exec.resourcemgr.rmblobmgr.rmblob.ClusterStateBlob;
import org.apache.drill.exec.resourcemgr.rmblobmgr.rmblob.ForemanQueueUsageBlob;
import org.apache.drill.exec.resourcemgr.rmblobmgr.rmblob.ForemanResourceUsage;
import org.apache.drill.exec.resourcemgr.rmblobmgr.rmblob.ForemanResourceUsage.ForemanResourceUsageDe;
import org.apache.drill.exec.resourcemgr.rmblobmgr.rmblob.QueueLeadershipBlob;
import org.apache.drill.exec.resourcemgr.rmblobmgr.rmblob.RMStateBlob;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.store.ZookeeperTransactionalPersistenceStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * RM state blobs manager which does all the update to the blobs under a global lock and in transactional manner.
 * Since the blobs are updated by multiple Drillbit at same time to maintain the strongly consistent information in
 * these blobs it uses a global lock shared across all the Drillbits.
 */
public class RMConsistentBlobStoreManager implements RMBlobStoreManager {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RMConsistentBlobStoreManager.class);

  private static final String RM_BLOBS_ROOT = "rm/blobs";

  private static final String RM_LOCK_ROOT = "/rm/locks";

  private static final String RM_BLOB_GLOBAL_LOCK_NAME = "/rm_blob_lock";

  private static final String RM_BLOB_SER_DE_NAME = "RMStateBlobSerDeModules";

  public static final int RM_STATE_BLOB_VERSION = 1;

  private static final int MAX_ACQUIRE_RETRY = 3;

  private final ZookeeperTransactionalPersistenceStore<RMStateBlob> rmBlobStore;

  private final InterProcessMutex globalBlobMutex;

  private final DrillbitContext context;

  private final ObjectMapper serDeMapper;

  private final Map<String, RMStateBlob> rmStateBlobs;

  private final StringBuilder exceptionStringBuilder = new StringBuilder();

  public RMConsistentBlobStoreManager(DrillbitContext context, Collection<QueryQueueConfig> leafQueues) throws
    StoreException {
    try {
      this.context = context;
      this.serDeMapper = initializeMapper(context.getClasspathScan());
      this.rmBlobStore = (ZookeeperTransactionalPersistenceStore<RMStateBlob>) context.getStoreProvider()
        .getOrCreateStore(PersistentStoreConfig.newJacksonBuilder(serDeMapper, RMStateBlob.class)
          .name(RM_BLOBS_ROOT)
          .persistWithTransaction()
          .build());
      this.globalBlobMutex = new InterProcessMutex(((ZKClusterCoordinator) context.getClusterCoordinator()).getCurator(),
        RM_LOCK_ROOT + RM_BLOB_GLOBAL_LOCK_NAME);
      this.rmStateBlobs = new HashMap<>();
      initializeBlobs(leafQueues);
    } catch (StoreException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new StoreException("Failed to initialize RM State Blobs", ex);
    }
  }

  private Collection<Class<?>> getAllBlobSubTypes(ScanResult classpathScan) {
    return new ArrayList<>(classpathScan.getImplementations(RMStateBlob.class));
  }

  private ObjectMapper initializeMapper(ScanResult scanResult) {
    final ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
    mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
    mapper.registerSubtypes(getAllBlobSubTypes(scanResult));

    final SimpleModule deserModule = new SimpleModule(RM_BLOB_SER_DE_NAME)
      .addDeserializer(NodeResources.class, new NodeResourcesDe())
      .addDeserializer(ForemanResourceUsage.class, new ForemanResourceUsageDe());
    mapper.registerModule(deserModule);
    return mapper;
  }

  private void initializeBlobs(Collection<QueryQueueConfig> leafQueues) throws Exception {
    // acquire the global lock and ensure that all the blobs are created with empty data
    int acquireTry = 1;
    do {
      try {
        globalBlobMutex.acquire();
        break;
      } catch (Exception ex) {
        ++acquireTry;
      }
    } while (acquireTry <= MAX_ACQUIRE_RETRY);

    // if the lock is not acquired then just return as some other Drillbit can do it
    // but there can be issues when none of the Drillbit is able to perform this operation
    if (!globalBlobMutex.isAcquiredInThisProcess()) {
      logger.warn("Failed to acquire global rm blobs lock to do blob initialization. Expectation is some other " +
        "Drillbit should be able to do it");
      return;
    }

    try {
      logger.info("Acquired global rm blobs lock to do blob initialization");
      // if here that means lock is acquired
      rmStateBlobs.put(ClusterStateBlob.NAME,
        new ClusterStateBlob(RM_STATE_BLOB_VERSION, new HashMap<>()));
      rmStateBlobs.put(QueueLeadershipBlob.NAME,
        new QueueLeadershipBlob(RM_STATE_BLOB_VERSION, new HashMap<>()));

      // This ForemanResourceUsage blob needs to be per queue
      final ForemanQueueUsageBlob queueUsageBlob = new ForemanQueueUsageBlob(RM_STATE_BLOB_VERSION, new HashMap<>());
      for (QueryQueueConfig queueConfig : leafQueues) {
        final String blobName = ForemanQueueUsageBlob.NAME + "_" + queueConfig.getQueueName();
        rmStateBlobs.put(blobName, queueUsageBlob);
      }

      for (Map.Entry<String, RMStateBlob> stateBlob : rmStateBlobs.entrySet()) {
        if (!rmBlobStore.putIfAbsent(stateBlob.getKey(), stateBlob.getValue())) {
          logger.info("Blob {} was already initialized", stateBlob.getKey());
        }
      }
    } catch (Exception ex) {
      // consume the exception during blob initialization since we are expecting some other Drillbit can do that
      // successfully. If not then there will be failure in cluster during actual blob update
      logger.error("Failed to initialize one or more blob with empty data, but consuming this exception since " +
        "expectation is that some other Drillbit should be able to perform this step");
    } finally {
      // throwing exception on release since it indicates mutex is in bad state
      globalBlobMutex.release();
    }
  }

  @Override
  public void reserveResources(Map<String, NodeResources> queryResourceAssignment,
                               QueryQueueConfig selectedQueue, String leaderId,
                               String queryId, String foremanNode) throws Exception {
    // Looks like leader hasn't changed yet so let's try to reserve the resources
    // See if the call is to reserve or free up resources
    Map<String, NodeResources> resourcesMap = queryResourceAssignment;
    resourcesMap = queryResourceAssignment.entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getKey,
        (x) -> new NodeResources(x.getValue().getVersion(),
                                 -x.getValue().getMemoryInBytes(),
                                 -x.getValue().getNumVirtualCpu())));
    acquireLockAndUpdate(resourcesMap, selectedQueue, leaderId, queryId, foremanNode);
  }

  @Override
  public void freeResources(Map<String, NodeResources> queryResourceAssignment,
                            QueryQueueConfig selectedQueue, String leaderId,
                            String queryId, String foremanNode) throws Exception {
    acquireLockAndUpdate(queryResourceAssignment, selectedQueue, leaderId, queryId, foremanNode);
  }

  private void updateBlobs(Map<String, NodeResources> resourcesMap, QueryQueueConfig selectedQueue,
                           String leaderId, String queryId, String foremanNode) throws Exception {

    exceptionStringBuilder.append("QueryId: ").append(queryId)
      .append(", ForemanBit: ").append(foremanNode)
      .append(", QueueName: ").append(selectedQueue.getQueueName())
      .append(", Admitted Leader: ").append(leaderId);

    // get all the required blobs data as a transaction. Note: We won't use getAll since we have queue specific
    // blobs too and the update will only for selected queue. So we should get data one blob at a time
    final List<String> blobsToGet = new ArrayList<>();
    final String queueBlobName = ForemanQueueUsageBlob.NAME + "_" + selectedQueue.getQueueName();
    blobsToGet.add(ClusterStateBlob.NAME);
    blobsToGet.add(QueueLeadershipBlob.NAME);
    blobsToGet.add(queueBlobName);

    final Map<String, RMStateBlob> rmBlobs = rmBlobStore.getAllOrNone(blobsToGet);
    if (rmBlobs == null) {
      throw new RMBlobUpdateException(String.format("Failed to get one or more blob while update. [Details: %s]",
        exceptionStringBuilder.toString()));
    } else {
      rmStateBlobs.putAll(rmBlobs);
    }

    // Check if the leader admitting the query is still leader of the queue
    final String currentQueueLeader = ((QueueLeadershipBlob)rmStateBlobs.get(QueueLeadershipBlob.NAME))
      .getQueueLeaders().get(selectedQueue.getQueueName());
    if (currentQueueLeader == null || !currentQueueLeader.equals(leaderId)) {
      throw new LeaderChangeException(String.format("The leader which admitted the query in queue doesn't match " +
        "current leader %s of the queue [Details: %s]", currentQueueLeader, exceptionStringBuilder.toString()));
    }
    // Remove leadership blob from cache
    rmStateBlobs.remove(QueueLeadershipBlob.NAME);

    // Cluster state blob
    final ClusterStateBlob currentClusterBlob = (ClusterStateBlob)rmStateBlobs.get(ClusterStateBlob.NAME);
    final Map<String, NodeResources> currentClusterState = currentClusterBlob.getClusterState();

    // ForemanResourceUsage blob
    final ForemanQueueUsageBlob resourceUsageBlob = (ForemanQueueUsageBlob)rmStateBlobs.get(queueBlobName);
    final Map<String, ForemanResourceUsage> allForemanUsage = resourceUsageBlob.getAllForemanInfo();
    final ForemanResourceUsage currentUsage = allForemanUsage.get(foremanNode);
    final Map<String, NodeResources> usageMapAcrossDrillbits = currentUsage.getForemanUsage();
    int currentRunningCount = currentUsage.getRunningCount();

    for (Map.Entry<String, NodeResources> nodeToUpdate : resourcesMap.entrySet()) {
      final String bitUUID = nodeToUpdate.getKey();
      final NodeResources bitResourcesToReserve = nodeToUpdate.getValue();

      final long memoryToReserve = bitResourcesToReserve.getMemoryInBytes();
      if (!currentClusterState.containsKey(bitUUID)) {
        throw new RMBlobUpdateException(String.format("Drillbit with UUID %s which is assigned to query is " +
          "not found in ClusterState blob. [Details: %s]", bitUUID, exceptionStringBuilder.toString()));
      }
      final NodeResources bitAvailableResources = currentClusterState.get(bitUUID);
      long currentAvailableMemory = bitAvailableResources.getMemoryInBytes();
      if (currentAvailableMemory < memoryToReserve) {
        throw new ResourceUnavailableException(String.format("Drillbit with UUID %s which is assigned to query " +
            "doesn't have enough memory available. [Details: %s, AvailableMemory: %s, RequiredMemory: %s]", bitUUID,
          currentAvailableMemory, memoryToReserve, exceptionStringBuilder.toString()));
      }
      // Update local ClusterState
      bitAvailableResources.setMemoryInBytes(currentAvailableMemory - memoryToReserve);
      currentClusterState.put(bitUUID, bitAvailableResources);

      // Update local ForemanResourceUsage for foremanNode with this query resource ask
      final NodeResources currentState = usageMapAcrossDrillbits.get(bitUUID);
      long availableMemory = currentState.getMemoryInBytes();
      currentState.setMemoryInBytes(availableMemory - memoryToReserve);
      usageMapAcrossDrillbits.put(bitUUID, currentState);
    }

    // update the local ClusterStateBlob with new information
    currentClusterBlob.setClusterState(currentClusterState);

    // update the local ForemanQueueUsageBlob with final ForemanResourceUsage
    currentUsage.setRunningCount(currentRunningCount + 1);
    currentUsage.setForemanUsage(usageMapAcrossDrillbits);
    allForemanUsage.put(foremanNode, currentUsage);
    resourceUsageBlob.setAllForemanInfo(allForemanUsage);

    // Update local blob cache
    rmStateBlobs.put(ClusterStateBlob.NAME, currentClusterBlob);
    rmStateBlobs.put(queueBlobName, resourceUsageBlob);

    // Persist the new blobs to Zookeeper
    if (!writeAllRMBlobs(rmStateBlobs)) {
      logger.error("Failed to update the cluster state blob and queue blob for queue {} in a transaction",
        selectedQueue.getQueueName());
      throw new RMBlobUpdateException(String.format("Failed to update the cluster state blob and queue blob in a " +
        "transaction. [Details: %s]", exceptionStringBuilder.toString()));
    }
    logger.debug("Successfully updated the blobs in a transaction. [Details: %s]", exceptionStringBuilder.toString());

    // Reset the exceptionStringBuilder for next event
    exceptionStringBuilder.delete(0, exceptionStringBuilder.length());
  }

  private void acquireLockAndUpdate(Map<String, NodeResources> queryResourceAssignment, QueryQueueConfig selectedQueue,
                                   String leaderId, String queryId, String foremanNode) throws Exception {
    try {
      globalBlobMutex.acquire();
    } catch (Exception ex) {
      logger.error("Failed on acquiring the global mutex while updating the RM blobs during update of resources");
      throw ex;
    }

    try {
      updateBlobs(queryResourceAssignment, selectedQueue, leaderId, queryId, foremanNode);
    } catch (Exception ex) {
      logger.error("Failed to update the blobs", ex);
      throw ex;
    } finally {
      // Check if the caller has acquired the mutex
      if (globalBlobMutex.isAcquiredInThisProcess()) {
        try {
          globalBlobMutex.release();
        } catch (Exception ex) {
          logger.error("Failed on releasing the global mutex while updating the RM blobs during update of resources",
            ex);
          // don't throw this release exception instead throw the original exception if any. Since release exception
          // should not matter much
        }
      }
    }
  }

  @VisibleForTesting
  public Iterator<Map.Entry<String, RMStateBlob>> readAllRMBlobs() {
    return rmBlobStore.getAll();
  }

  @VisibleForTesting
  public boolean writeAllRMBlobs(Map<String, RMStateBlob> rmStateBlobs) {
    return rmBlobStore.putAsTransaction(rmStateBlobs);
  }

  @VisibleForTesting
  public Map<String, String> serializePassedInBlob(Map<String, RMStateBlob> inputBlobs) throws Exception {
    Map<String, String> serializedBlobs = new HashMap<>();
    for (Map.Entry<String, RMStateBlob> blobEntry : inputBlobs.entrySet()) {
      serializedBlobs.put(blobEntry.getKey(), serDeMapper.writeValueAsString(blobEntry.getValue()));
    }
    return serializedBlobs;
  }

  @VisibleForTesting
  public Map<String, RMStateBlob> deserializeRMStateBlobs(Map<String, String> blobsInfo) throws Exception {
    Map<String, RMStateBlob> deserializedBlobs = new HashMap<>();
    for (Map.Entry<String, String> blobEntry : blobsInfo.entrySet()) {
      deserializedBlobs.put(blobEntry.getKey(), serDeMapper.readValue(blobEntry.getValue(),
        RMStateBlob.class));
    }
    return deserializedBlobs;
  }
}
