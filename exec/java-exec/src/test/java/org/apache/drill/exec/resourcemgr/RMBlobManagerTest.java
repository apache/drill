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
package org.apache.drill.exec.resourcemgr;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.resourcemgr.config.QueryQueueConfig;
import org.apache.drill.exec.resourcemgr.config.QueryQueueConfigImpl;
import org.apache.drill.exec.resourcemgr.config.ResourcePoolTree;
import org.apache.drill.exec.resourcemgr.config.ResourcePoolTreeImpl;
import org.apache.drill.exec.resourcemgr.rmblobmgr.RMConsistentBlobStoreManager;
import org.apache.drill.exec.resourcemgr.rmblobmgr.exception.LeaderChangeException;
import org.apache.drill.exec.resourcemgr.rmblobmgr.exception.RMBlobUpdateException;
import org.apache.drill.exec.resourcemgr.rmblobmgr.exception.ResourceUnavailableException;
import org.apache.drill.exec.resourcemgr.rmblobmgr.rmblob.ClusterStateBlob;
import org.apache.drill.exec.resourcemgr.rmblobmgr.rmblob.ForemanQueueUsageBlob;
import org.apache.drill.exec.resourcemgr.rmblobmgr.rmblob.ForemanResourceUsage;
import org.apache.drill.exec.resourcemgr.rmblobmgr.rmblob.QueueLeadershipBlob;
import org.apache.drill.exec.resourcemgr.rmblobmgr.rmblob.RMStateBlob;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.DrillTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RMBlobManagerTest extends DrillTest {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RMBlobManagerTest.class);
  @Rule
  public final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

  private Config queueConfig;

  private RMConsistentBlobStoreManager rmConsistentBlobStoreManager;

  private final NodeResources nodeResourceShare = new NodeResources(65535, 10);

  private ClusterStateBlob clusterStateBlob;

  private QueueLeadershipBlob queueLeadershipBlob;

  private ForemanQueueUsageBlob foremanQueueUsageBlob;

  private ForemanResourceUsage foreman1RsrcUsage;

  private ForemanResourceUsage foreman2RsrcUsage;

  private ForemanResourceUsage foreman3RsrcUsage;

  private final Map<String, QueryQueueConfig> leafQueues = new HashMap<>();

  private final List<String> drillUUID = new ArrayList<>();

  private final Map<String, RMStateBlob> blobsToSerialize = new HashMap<>();

  private ClusterFixture cluster;

  @Before
  public void testSetup() throws Exception {
    final Map<String, Object> queueConfigValues = new HashMap<>();
    queueConfigValues.put(QueryQueueConfigImpl.MAX_QUERY_MEMORY_PER_NODE_KEY, "8192K");

    queueConfig = ConfigFactory.empty().withValue("queue", ConfigValueFactory.fromMap(queueConfigValues));

    final QueryQueueConfig leafQueue1 = new QueryQueueConfigImpl(queueConfig.getConfig("queue"), "queue1",
      null);
    final QueryQueueConfig leafQueue2 = new QueryQueueConfigImpl(queueConfig.getConfig("queue"), "queue2",
      null);
    final QueryQueueConfig leafQueue3 = new QueryQueueConfigImpl(queueConfig.getConfig("queue"), "queue3",
      null);

    leafQueues.put("queue1", leafQueue1);
    leafQueues.put("queue2", leafQueue2);
    leafQueues.put("queue3", leafQueue3);

    drillUUID.add(UUID.randomUUID().toString());
    drillUUID.add(UUID.randomUUID().toString());
    drillUUID.add(UUID.randomUUID().toString());

    final Map<String, NodeResources> clusterStateValue = new HashMap<>();
    clusterStateValue.put(drillUUID.get(0), nodeResourceShare);
    clusterStateValue.put(drillUUID.get(1), nodeResourceShare);
    clusterStateValue.put(drillUUID.get(2), nodeResourceShare);

    final Map<String, String> queueLeadersValue = new HashMap<>();
    queueLeadersValue.put(leafQueue1.getQueueName(), drillUUID.get(0));
    queueLeadersValue.put(leafQueue2.getQueueName(), drillUUID.get(1));
    queueLeadersValue.put(leafQueue3.getQueueName(), drillUUID.get(2));

    final Map<String, NodeResources> foreman1Usage = new HashMap<>();
    final NodeResources foreman1Resource = new NodeResources(1000, 1);
    foreman1Usage.put(drillUUID.get(0), foreman1Resource);
    foreman1Usage.put(drillUUID.get(1), foreman1Resource);
    foreman1Usage.put(drillUUID.get(2), foreman1Resource);

    final Map<String, NodeResources> foreman2Usage = new HashMap<>();
    final NodeResources foreman2Resource = new NodeResources(2000, 1);
    foreman2Usage.put(drillUUID.get(0), foreman2Resource);
    foreman2Usage.put(drillUUID.get(1), foreman2Resource);
    foreman2Usage.put(drillUUID.get(2), foreman2Resource);

    final Map<String, NodeResources> foreman3Usage = new HashMap<>();
    final NodeResources foreman3Resource = new NodeResources(3000, 1);
    foreman3Usage.put(drillUUID.get(0), foreman3Resource);
    foreman3Usage.put(drillUUID.get(1), foreman3Resource);
    foreman3Usage.put(drillUUID.get(2), foreman3Resource);

    foreman1RsrcUsage = new ForemanResourceUsage(RMConsistentBlobStoreManager.RM_STATE_BLOB_VERSION,
      foreman1Usage, 1);
    foreman2RsrcUsage = new ForemanResourceUsage(RMConsistentBlobStoreManager.RM_STATE_BLOB_VERSION,
      foreman2Usage, 2);
    foreman3RsrcUsage = new ForemanResourceUsage(RMConsistentBlobStoreManager.RM_STATE_BLOB_VERSION,
      foreman3Usage, 3);

    final Map<String, ForemanResourceUsage> formemanQueueUsageValues = new HashMap<>();
    formemanQueueUsageValues.put(drillUUID.get(0), foreman1RsrcUsage);
    formemanQueueUsageValues.put(drillUUID.get(1), foreman2RsrcUsage);
    formemanQueueUsageValues.put(drillUUID.get(2), foreman3RsrcUsage);

    clusterStateBlob = new ClusterStateBlob(RMConsistentBlobStoreManager.RM_STATE_BLOB_VERSION, clusterStateValue);
    queueLeadershipBlob = new QueueLeadershipBlob(RMConsistentBlobStoreManager.RM_STATE_BLOB_VERSION,
      queueLeadersValue);
    foremanQueueUsageBlob = new ForemanQueueUsageBlob(RMConsistentBlobStoreManager.RM_STATE_BLOB_VERSION,
      formemanQueueUsageValues);

    final ClusterFixtureBuilder fixtureBuilder = ClusterFixture.builder(dirTestWatcher)
      .configProperty(ExecConstants.RM_ENABLED, false)
      .setOptionDefault(ExecConstants.ENABLE_QUEUE.getOptionName(), false)
      .configProperty(ExecConstants.DRILL_PORT_HUNT, true)
      .configProperty(ExecConstants.ALLOW_LOOPBACK_ADDRESS_BINDING, true)
      .withLocalZk();

    // start the cluster
    cluster = fixtureBuilder.build();

    // prepare the blob cache
    blobsToSerialize.put(ClusterStateBlob.NAME, clusterStateBlob);
    blobsToSerialize.put(QueueLeadershipBlob.NAME, queueLeadershipBlob);

    for (QueryQueueConfig leafQueue : leafQueues.values()) {
      String blobName = ForemanQueueUsageBlob.NAME + "_" + leafQueue.getQueueName();
      blobsToSerialize.put(blobName, foremanQueueUsageBlob);
    }

    // initialize the blobs
    final DrillbitContext context = cluster.drillbit().getContext();
    final ResourcePoolTree rmPoolTree = mock(ResourcePoolTreeImpl.class);
    when(rmPoolTree.getAllLeafQueues()).thenReturn(leafQueues);
    when(rmPoolTree.getRootPoolResources()).thenReturn(nodeResourceShare);
    rmConsistentBlobStoreManager = new RMConsistentBlobStoreManager(context, rmPoolTree);
    rmConsistentBlobStoreManager.writeAllRMBlobs(blobsToSerialize);
  }

  @After
  public void testCleanup() throws Exception {
    cluster.close();
  }

  private void verifyBlobs() {
    // Again verify the updated blob value with initial value
    Iterator<Map.Entry<String, RMStateBlob>> blobs = rmConsistentBlobStoreManager.readAllRMBlobs();
    while(blobs.hasNext()) {
      final Map.Entry<String, RMStateBlob> currentBlob = blobs.next();
      if (currentBlob.getKey().equals(ClusterStateBlob.NAME)) {
        final ClusterStateBlob newStateBlob = (ClusterStateBlob) currentBlob.getValue();
        assertTrue(clusterStateBlob.equals(newStateBlob));
      } else if (currentBlob.getKey().equals(QueueLeadershipBlob.NAME)) {
        assertTrue(queueLeadershipBlob.equals(currentBlob.getValue()));
      } else {
        assertTrue(foremanQueueUsageBlob.equals(currentBlob.getValue()));
      }
    }
  }

  @Test
  public void testRMStateBlobSerDe() throws Exception {
    final Map<String, String> serializedBlobs = rmConsistentBlobStoreManager.serializePassedInBlob(blobsToSerialize);
    final Map<String, RMStateBlob> deserializedBlobs =
      rmConsistentBlobStoreManager.deserializeRMStateBlobs(serializedBlobs);

    for (Map.Entry<String, RMStateBlob> blobEntry : deserializedBlobs.entrySet()) {
      final RMStateBlob actualBlob = blobEntry.getValue();
      assertEquals(blobsToSerialize.get(blobEntry.getKey()), actualBlob);
    }
  }

  @Test
  public void testSuccessfulReserveAndFree() throws Exception {
    // Now let's reserve some resources for a query through reserve api
    final Map<String, NodeResources> resourceToReserve = new HashMap<>();
    resourceToReserve.put(drillUUID.get(0), new NodeResources(15535, 1));
    resourceToReserve.put(drillUUID.get(1), new NodeResources(15535, 1));

    final String foremanUUID = drillUUID.get(1);
    final UserBitShared.QueryId queryId = UserBitShared.QueryId.getDefaultInstance();
    rmConsistentBlobStoreManager.reserveResources(resourceToReserve, leafQueues.get("queue1"),
      drillUUID.get(0), queryId.toString(), foremanUUID);

    // Verify the updated blob value with expected value
    Iterator<Map.Entry<String, RMStateBlob>> blobs = rmConsistentBlobStoreManager.readAllRMBlobs();
    while(blobs.hasNext()) {
      final Map.Entry<String, RMStateBlob> currentBlob = blobs.next();
      if (currentBlob.getKey().equals(ClusterStateBlob.NAME)) {
        final ClusterStateBlob newStateBlob = (ClusterStateBlob) currentBlob.getValue();
        final Map<String, NodeResources> clusterState = newStateBlob.getClusterState();
        assertEquals(
          nodeResourceShare.getMemoryInBytes() - resourceToReserve.get(drillUUID.get(0)).getMemoryInBytes(),
          clusterState.get(drillUUID.get(0)).getMemoryInBytes());
        assertEquals(
          nodeResourceShare.getMemoryInBytes() - resourceToReserve.get(drillUUID.get(1)).getMemoryInBytes(),
          clusterState.get(drillUUID.get(1)).getMemoryInBytes());
      } else if (currentBlob.getKey().equals(ForemanQueueUsageBlob.NAME + "_queue1")) {
        final ForemanQueueUsageBlob foremanUsage = (ForemanQueueUsageBlob) currentBlob.getValue();
        final ForemanResourceUsage queryForemanUsage = foremanUsage.getAllForemanInfo().get(foremanUUID);
        assertEquals(foreman2RsrcUsage.getRunningCount() + 1, queryForemanUsage.getRunningCount());
        final Map<String, NodeResources> otherDrillbitResourcesUsed = foreman2RsrcUsage.getForemanUsage();
        assertEquals(otherDrillbitResourcesUsed.get(drillUUID.get(0)).getMemoryInBytes() +
            resourceToReserve.get(drillUUID.get(0)).getMemoryInBytes(),
          queryForemanUsage.getForemanUsage().get(drillUUID.get(0)).getMemoryInBytes());
        assertEquals(otherDrillbitResourcesUsed.get(drillUUID.get(1)).getMemoryInBytes() +
            resourceToReserve.get(drillUUID.get(1)).getMemoryInBytes(),
          queryForemanUsage.getForemanUsage().get(drillUUID.get(1)).getMemoryInBytes());
      }
    }

    // release the resource back
    rmConsistentBlobStoreManager.freeResources(resourceToReserve, leafQueues.get("queue1"),
      drillUUID.get(0), queryId.toString(), foremanUUID);

    // Again verify the updated blob value with initial value
    verifyBlobs();
  }

  @Test (expected = RMBlobUpdateException.class)
  public void testNonExistingNodeDuringReserve() throws Exception {
    testNonExistingNodeCommon(false);
  }

  @Test
  public void testNonExistingNodeDuringFree() throws Exception {
    testNonExistingNodeCommon(true);
  }

  private void testNonExistingNodeCommon(boolean isFree) throws Exception {
    // Now let's reserve some resources for a query through reserve api
    final Map<String, NodeResources> resourceToReserve = new HashMap<>();
    resourceToReserve.put(UUID.randomUUID().toString(),
      new NodeResources(nodeResourceShare.getMemoryInBytes() + 1, 1));
    resourceToReserve.put(drillUUID.get(1), new NodeResources(nodeResourceShare.getMemoryInBytes(), 1));

    final String foremanUUID = drillUUID.get(1);
    final UserBitShared.QueryId queryId = UserBitShared.QueryId.getDefaultInstance();
    if (isFree) {
      rmConsistentBlobStoreManager.freeResources(resourceToReserve, leafQueues.get("queue1"), drillUUID.get(0),
        queryId.toString(), foremanUUID);
    } else {
      rmConsistentBlobStoreManager.reserveResources(resourceToReserve, leafQueues.get("queue1"), drillUUID.get(0),
        queryId.toString(), foremanUUID);
    }
  }

  @Test (expected = LeaderChangeException.class)
  public void testLeaderChangeForQueueOnReserve() throws Exception {
    testLeaderChangeCommon(false);
  }

  @Test
  public void testLeaderChangeForQueueOnFree() throws Exception {
    testLeaderChangeCommon(true);
    verifyBlobs();
  }

  private void testLeaderChangeCommon(boolean isFree) throws Exception {
    // First reserve some resources for a query through reserve api
    final Map<String, NodeResources> resourceToReserve = new HashMap<>();
    resourceToReserve.put(drillUUID.get(0), new NodeResources(4000, 1));
    resourceToReserve.put(drillUUID.get(1), new NodeResources(4000, 1));

    final String foremanUUID = drillUUID.get(1);
    final String leaderUUID = drillUUID.get(0);
    final UserBitShared.QueryId queryId = UserBitShared.QueryId.getDefaultInstance();
    rmConsistentBlobStoreManager.reserveResources(resourceToReserve, leafQueues.get("queue1"),
      (isFree) ? leaderUUID : UUID.randomUUID().toString(), queryId.toString(), foremanUUID);

    // now free up the query reserved resources
    rmConsistentBlobStoreManager.freeResources(resourceToReserve, leafQueues.get("queue1"),
      (isFree) ? UUID.randomUUID().toString() : leaderUUID, queryId.toString(), foremanUUID);
  }

  @Test (expected = ResourceUnavailableException.class)
  public void testReserveMoreThanAllowedForANode() throws Exception {
    // Now let's reserve some resources for a query through reserve api
    final Map<String, NodeResources> resourceToReserve = new HashMap<>();
    resourceToReserve.put(drillUUID.get(0), new NodeResources(nodeResourceShare.getMemoryInBytes() + 1,
      1));
    resourceToReserve.put(drillUUID.get(1), new NodeResources(nodeResourceShare.getMemoryInBytes(), 1));

    final String foremanUUID = drillUUID.get(1);
    final UserBitShared.QueryId queryId = UserBitShared.QueryId.getDefaultInstance();
    rmConsistentBlobStoreManager.reserveResources(resourceToReserve, leafQueues.get("queue1"),
      drillUUID.get(0), queryId.toString(), foremanUUID);
  }

  @Test (expected = RMBlobUpdateException.class)
  public void testBlobAbsentBeforeUpdate() throws Exception {
    try {
      final Map<String, NodeResources> resourceToReserve = new HashMap<>();
      resourceToReserve.put(drillUUID.get(0), new NodeResources(nodeResourceShare.getMemoryInBytes() + 1,
        1));
      resourceToReserve.put(drillUUID.get(1), new NodeResources(nodeResourceShare.getMemoryInBytes(), 1));

      final String foremanUUID = drillUUID.get(1);
      final UserBitShared.QueryId queryId = UserBitShared.QueryId.getDefaultInstance();

      rmConsistentBlobStoreManager.deleteAllRMBlobs(new ArrayList<>(blobsToSerialize.keySet()));
      rmConsistentBlobStoreManager.reserveResources(resourceToReserve, leafQueues.get("queue1"), drillUUID.get(0),
        queryId.toString(), foremanUUID);
    } finally {
      // restore the blobs
      rmConsistentBlobStoreManager.writeAllRMBlobs(blobsToSerialize);
    }
  }
}
