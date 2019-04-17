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
import org.apache.drill.exec.resourcemgr.config.QueryQueueConfig;
import org.apache.drill.exec.resourcemgr.config.QueryQueueConfigImpl;
import org.apache.drill.exec.resourcemgr.rmblobmgr.RMConsistentBlobStoreManager;
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class RMBlobManagerTest extends DrillTest {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RMBlobManagerTest.class);
  @Rule
  public final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

  private Config queueConfig;

  private RMConsistentBlobStoreManager rmConsistentBlobStoreManager;

  private ClusterStateBlob clusterStateBlob;

  private QueueLeadershipBlob queueLeadershipBlob;

  private ForemanQueueUsageBlob foremanQueueUsageBlob;

  private final List<QueryQueueConfig> leafQueues = new ArrayList<>();

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

      leafQueues.add(leafQueue1);
      leafQueues.add(leafQueue2);
      leafQueues.add(leafQueue3);

      final List<String> drillUUID = new ArrayList<>();
      drillUUID.add(UUID.randomUUID().toString());
      drillUUID.add(UUID.randomUUID().toString());
      drillUUID.add(UUID.randomUUID().toString());

      final Map<String, NodeResources> clusterStateValue = new HashMap<>();
      clusterStateValue.put(drillUUID.get(0), new NodeResources(65535, 10));
      clusterStateValue.put(drillUUID.get(1), new NodeResources(65535, 10));
      clusterStateValue.put(drillUUID.get(2), new NodeResources(65535, 10));

      final Map<String, String> queueLeadersValue = new HashMap<>();
      queueLeadersValue.put(leafQueue1.getQueueName(), drillUUID.get(0));
      queueLeadersValue.put(leafQueue2.getQueueName(), drillUUID.get(1));
      queueLeadersValue.put(leafQueue3.getQueueName(), drillUUID.get(2));

      final Map<String, NodeResources> foreman1Usage = new HashMap<>();
      foreman1Usage.put(drillUUID.get(1), new NodeResources(1000, 1));
      foreman1Usage.put(drillUUID.get(2), new NodeResources(2000, 1));

      final Map<String, NodeResources> foreman2Usage = new HashMap<>();
      foreman2Usage.put(drillUUID.get(0), new NodeResources(1000, 1));
      foreman2Usage.put(drillUUID.get(2), new NodeResources(2000, 1));

      final Map<String, NodeResources> foreman3Usage = new HashMap<>();
      foreman3Usage.put(drillUUID.get(0), new NodeResources(1000, 1));
      foreman3Usage.put(drillUUID.get(1), new NodeResources(2000, 1));


      final ForemanResourceUsage foreman1 = new ForemanResourceUsage(RMConsistentBlobStoreManager.RM_STATE_BLOB_VERSION,
        foreman1Usage, 1);
      final ForemanResourceUsage foreman2 = new ForemanResourceUsage(RMConsistentBlobStoreManager.RM_STATE_BLOB_VERSION,
        foreman2Usage, 2);
      final ForemanResourceUsage foreman3 = new ForemanResourceUsage(RMConsistentBlobStoreManager.RM_STATE_BLOB_VERSION,
        foreman3Usage, 3);

      final Map<String, ForemanResourceUsage> formemanQueueUsageValues = new HashMap<>();
      formemanQueueUsageValues.put(drillUUID.get(0), foreman1);
      formemanQueueUsageValues.put(drillUUID.get(1), foreman2);
      formemanQueueUsageValues.put(drillUUID.get(2), foreman3);

      clusterStateBlob = new ClusterStateBlob(RMConsistentBlobStoreManager.RM_STATE_BLOB_VERSION,
        clusterStateValue);
      queueLeadershipBlob = new QueueLeadershipBlob(RMConsistentBlobStoreManager.RM_STATE_BLOB_VERSION,
        queueLeadersValue);
      foremanQueueUsageBlob = new ForemanQueueUsageBlob(RMConsistentBlobStoreManager.RM_STATE_BLOB_VERSION,
        formemanQueueUsageValues);
  }

  @Test
  public void testRMStateBlobSerDe() throws Exception {
    ClusterFixtureBuilder fixtureBuilder = ClusterFixture.builder(dirTestWatcher)
      .configProperty(ExecConstants.DRILL_PORT_HUNT, true)
      .withLocalZk();

    try (ClusterFixture cluster = fixtureBuilder.build()) {
      final DrillbitContext context = cluster.drillbit().getContext();
      rmConsistentBlobStoreManager = new RMConsistentBlobStoreManager(context, leafQueues);
      Map<String, RMStateBlob> blobsToSerialize = new HashMap<>();
      blobsToSerialize.put(ClusterStateBlob.NAME, clusterStateBlob);
      blobsToSerialize.put(QueueLeadershipBlob.NAME, queueLeadershipBlob);

      for (QueryQueueConfig leafQueue : leafQueues) {
        String blobName = ForemanQueueUsageBlob.NAME + "_" + leafQueue.getQueueName();
        blobsToSerialize.put(blobName, foremanQueueUsageBlob);
      }

      final Map<String, String> serializedBlobs = rmConsistentBlobStoreManager.serializePassedInBlob(blobsToSerialize);
      final Map<String, RMStateBlob> deserializedBlobs = rmConsistentBlobStoreManager.deserializeRMStateBlobs(serializedBlobs);

      for (Map.Entry<String, RMStateBlob> blobEntry : deserializedBlobs.entrySet()) {
        final RMStateBlob actualBlob = blobEntry.getValue();
        assertEquals(blobsToSerialize.get(blobEntry.getKey()), actualBlob);
      }
    }
  }

  public void testBlobManagerReserveApi() throws Exception {
    ClusterFixtureBuilder fixtureBuilder = ClusterFixture.builder(dirTestWatcher)
      .configProperty(ExecConstants.DRILL_PORT_HUNT, true)
      .withLocalZk();

    try (ClusterFixture cluster = fixtureBuilder.build()) {
      DrillbitContext context = cluster.drillbit().getContext();
      final RMConsistentBlobStoreManager rmManager = new RMConsistentBlobStoreManager(context, leafQueues);

    }
  }
}
