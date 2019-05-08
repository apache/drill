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

import org.apache.drill.common.DrillNode;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.drill.exec.work.foreman.rm.DistributedResourceManager;
import org.apache.drill.exec.work.foreman.rm.QueryResourceManager;
import org.apache.drill.exec.work.foreman.rm.ResourceManager;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestDistributedQueryRM extends ClusterTest {

  private static DistributedResourceManager drillRM;

  private static DrillbitContext context;

  private static Foreman mockForeman;

  private static final NodeResources testResources = new NodeResources(100, 1);

  private static final Map<String, NodeResources> queryCosts = new HashMap<>();

  private DistributedResourceManager.DistributedQueryRM queryRM;

  @BeforeClass
  public static void setupTestSuite() throws Exception {
    final ClusterFixtureBuilder fixtureBuilder = ClusterFixture.builder(dirTestWatcher)
      .configProperty(ExecConstants.RM_ENABLED, true)
      .setOptionDefault(ExecConstants.ENABLE_QUEUE.getOptionName(), false)
      .configProperty(ExecConstants.DRILL_PORT_HUNT, true)
      .configProperty(ExecConstants.ALLOW_LOOPBACK_ADDRESS_BINDING, true)
      .withLocalZk();
    startCluster(fixtureBuilder);
    context = cluster.drillbit().getContext();
    final ResourceManager rm = context.getResourceManager();
    Preconditions.checkState(rm instanceof DistributedResourceManager);
    drillRM = (DistributedResourceManager) rm;

    final Map<CoordinationProtos.DrillbitEndpoint, String> onlineEndpoints = context.getOnlineEndpointUUIDs();
    final Map<DrillNode, String> onlineEndpointNodes = onlineEndpoints.entrySet().stream()
      .collect(Collectors.toMap(x -> DrillNode.create(x.getKey()), x -> x.getValue()));

    mockForeman = mock(Foreman.class);
    final QueryContext queryContext = mock(QueryContext.class);
    when(mockForeman.getQueryContext()).thenReturn(queryContext);
    when(queryContext.getQueryId()).thenReturn(UserBitShared.QueryId.getDefaultInstance());
    when(queryContext.getOnlineEndpointUUIDs()).thenReturn(context.getOnlineEndpointUUIDs());
    when(queryContext.getOnlineEndpointNodeUUIDs()).thenReturn(onlineEndpointNodes);
    when(queryContext.getCurrentEndpoint()).thenReturn(context.getEndpoint());
    when(queryContext.getCurrentEndpointNode()).thenReturn(DrillNode.create(context.getEndpoint()));
    when(queryContext.getQueryUserName()).thenReturn(System.getProperty("user.name"));
    final Collection<String> keyUUIDs = context.getOnlineEndpointUUIDs().values();
    for (String keyID : keyUUIDs) {
      queryCosts.put(keyID, testResources);
    }
  }

  @Before
  public void testSetup() throws Exception {
    queryRM = (DistributedResourceManager.DistributedQueryRM) drillRM.newQueryRM(mockForeman);
  }

  @Test (expected = IllegalStateException.class)
  public void testQueryRMReserve_NoQueue_NoCost() throws Exception {
    queryRM.updateState(QueryResourceManager.QueryRMState.ENQUEUED);
    // don't select queue and set cost for this query before reserving resources
    queryRM.reserveResources();
  }

  @Test (expected = IllegalStateException.class)
  public void testQueryRMReserve_NoCost() throws Exception {
    // don't set cost for this query before reserving resources
    queryRM.selectQueue(testResources);
    queryRM.updateState(QueryResourceManager.QueryRMState.ENQUEUED);
    queryRM.reserveResources();
  }

  @Test (expected = IllegalStateException.class)
  public void testQueryRMReserve_BeforeAdmit() throws Exception {
    // don't admit this query before reserving resources
    queryRM.selectQueue(testResources);
    queryRM.setCost(queryCosts);
    queryRM.updateState(QueryResourceManager.QueryRMState.ENQUEUED);
    queryRM.reserveResources();
  }

  @Test
  public void testQueryRMReserveSuccess() throws Exception {
    queryRM.selectQueue(testResources);
    queryRM.setCost(queryCosts);
    queryRM.updateState(QueryResourceManager.QueryRMState.ENQUEUED);
    queryRM.updateState(QueryResourceManager.QueryRMState.ADMITTED);
    queryRM.reserveResources();
    assertTrue(queryRM.getCurrentState() == QueryResourceManager.QueryRMState.RESERVED_RESOURCES);
    queryRM.exit();
    assertTrue(queryRM.getCurrentState() == QueryResourceManager.QueryRMState.COMPLETED);
  }

  @Test
  public void testQueryRMExitInStartingState_QueryFailed() throws Exception {
    when(mockForeman.getState()).thenReturn(UserBitShared.QueryResult.QueryState.FAILED);
    queryRM.exit();
    assertTrue(queryRM.getCurrentState() == QueryResourceManager.QueryRMState.FAILED);
  }

  @Test
  public void testQueryRMExitInEnqueueState_QueryFailed() throws Exception {
    when(mockForeman.getState()).thenReturn(UserBitShared.QueryResult.QueryState.FAILED);
    queryRM.selectQueue(testResources);
    queryRM.setCost(queryCosts);
    queryRM.updateState(QueryResourceManager.QueryRMState.ENQUEUED);
    queryRM.exit();
    assertTrue(queryRM.getCurrentState() == QueryResourceManager.QueryRMState.FAILED);
  }

  @Test (expected = IllegalStateException.class)
  public void testQueryRMExitInStartingState_QueryPreparing() throws Exception {
    queryRM.exit();
  }

  @Test
  public void testQueryRMAdmitted() throws Exception {
    queryRM.selectQueue(testResources);
    queryRM.setCost(queryCosts);
    queryRM.admit();
    queryRM.updateState(QueryResourceManager.QueryRMState.ADMITTED);
    queryRM.exit();
    assertTrue(queryRM.getCurrentState() == QueryResourceManager.QueryRMState.COMPLETED);
  }
}
