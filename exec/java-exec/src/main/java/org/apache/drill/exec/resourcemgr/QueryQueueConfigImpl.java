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

import java.util.UUID;

public class QueryQueueConfigImpl implements QueryQueueConfig {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryQueueConfigImpl.class);

  private String queueUUID;

  private String queueName;

  private int maxAdmissibleQuery;

  private int maxWaitingQuery;

  private int maxWaitingTimeout;

  private boolean waitForPreferredNodes;

  private NodeResources queueResourceShare;

  private NodeResources queryPerNodeResourceShare;

  /** Optional queue configurations */
  private static final String MAX_ADMISSIBLE_KEY = "max_admissible";

  private static final String MAX_WAITING_KEY = "max_waiting";

  private static final String MAX_WAIT_TIMEOUT_KEY = "max_wait_timeout";

  private static final String WAIT_FOR_PREFERRED_NODES_KEY = "wait_for_preferred_nodes";

  /** Required queue configurations */
  private static final String MAX_QUERY_MEMORY_PER_NODE_KEY = "max_query_memory_per_node";

  public QueryQueueConfigImpl(Config queueConfig, String poolName,
                              NodeResources queueNodeResource) {
    this.queueUUID = UUID.randomUUID().toString();
    this.queueName = poolName;
    parseQueueConfig(queueConfig, queueNodeResource);
  }

  private void parseQueueConfig(Config queueConfig, NodeResources nodeShare) {
    try {
      this.queueResourceShare = nodeShare;
      this.maxAdmissibleQuery = queueConfig.hasPath(MAX_ADMISSIBLE_KEY) ? queueConfig.getInt(MAX_ADMISSIBLE_KEY) : 10;
      this.maxWaitingQuery = queueConfig.hasPath(MAX_WAITING_KEY) ? queueConfig.getInt(MAX_WAITING_KEY) : 5;
      this.maxWaitingTimeout = queueConfig.hasPath(MAX_WAIT_TIMEOUT_KEY)
        ? queueConfig.getInt(MAX_WAIT_TIMEOUT_KEY) : Integer.MAX_VALUE;
      this.waitForPreferredNodes = !queueConfig.hasPath(WAIT_FOR_PREFERRED_NODES_KEY) ||
        queueConfig.getBoolean(WAIT_FOR_PREFERRED_NODES_KEY);
      this.queryPerNodeResourceShare = new NodeResources(queueConfig.getLong(MAX_QUERY_MEMORY_PER_NODE_KEY), Integer
        .MAX_VALUE);
    } catch (Exception ex) {
      // TODO: Need to figure out exception logic
    }

  }

  @Override
  public String getQueueId() {
    return queueUUID;
  }

  @Override
  public String getQueueName() {
    return queueName;
  }

  @Override
  public long getQueueTotalMemoryInMB(int numClusterNodes) {
    return queueResourceShare.getMemoryInMB() * numClusterNodes;
  }

  @Override
  public long getMaxQueryMemoryInMBPerNode() {
    return queryPerNodeResourceShare.getMemoryInMB();
  }

  @Override
  public long getMaxQueryTotalMemoryInMB(int numClusterNodes) {
    return queryPerNodeResourceShare.getMemoryInMB() * numClusterNodes;
  }

  @Override
  public boolean waitForPreferredNodes() {
    return waitForPreferredNodes;
  }

  @Override
  public int getMaxAdmissibleQueries() {
    return maxAdmissibleQuery;
  }

  @Override
  public int getMaxWaitingQueries() {
    return maxWaitingQuery;
  }

  @Override
  public int getWaitTimeoutInMs() {
    return maxWaitingTimeout;
  }
}
