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
import org.apache.drill.exec.resourcemgr.exception.RMConfigException;

import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.drill.exec.resourcemgr.RMCommonDefaults.MAX_ADMISSIBLE_DEFAULT;
import static org.apache.drill.exec.resourcemgr.RMCommonDefaults.MAX_WAITING_DEFAULT;

/**
 * Parses and initialize QueueConfiguration for a {@link ResourcePool}. It also generates a unique UUID for each queue
 * which will be later used by Drillbit to store in Zookeeper along with
 * {@link org.apache.drill.exec.proto.beans.DrillbitEndpoint}. This UUID is used in the leader election mechanism in
 * which Drillbit participates for each of the configured rm queue.
 */
public class QueryQueueConfigImpl implements QueryQueueConfig {

  private String queueUUID;

  private String queueName;

  private int maxAdmissibleQuery;

  private int maxWaitingQuery;

  private int maxWaitingTimeout;

  private boolean waitForPreferredNodes;

  private NodeResources queueResourceShare;

  private NodeResources queryPerNodeResourceShare;

  // Optional queue configurations
  private static final String MAX_ADMISSIBLE_KEY = "max_admissible";

  private static final String MAX_WAITING_KEY = "max_waiting";

  private static final String MAX_WAIT_TIMEOUT_KEY = "max_wait_timeout";

  private static final String WAIT_FOR_PREFERRED_NODES_KEY = "wait_for_preferred_nodes";

  private static final String MAX_QUERY_MEMORY_PER_NODE_FORMAT = "([0-9]+)\\s*([kKmMgG]?)\\s*$";

  // Required queue configurations in MAX_QUERY_MEMORY_PER_NODE_FORMAT pattern
  private static final String MAX_QUERY_MEMORY_PER_NODE_KEY = "max_query_memory_per_node";

  public QueryQueueConfigImpl(Config queueConfig, String poolName,
                              NodeResources queueNodeResource) throws RMConfigException {
    this.queueUUID = UUID.randomUUID().toString();
    this.queueName = poolName;
    parseQueueConfig(queueConfig, queueNodeResource);
  }

  /**
   * Assigns either supplied or default values for the optional queue configuration. For required configuration uses
   * the supplied value in queueConfig object or throws proper exception if not present
   * @param queueConfig - Config object for ResourcePool queue
   * @param nodeShare - Share of resources on a node for this queue
   * @throws RMConfigException - In case of error
   */
  private void parseQueueConfig(Config queueConfig, NodeResources nodeShare) throws RMConfigException {
    this.queueResourceShare = nodeShare;
    this.maxAdmissibleQuery = queueConfig.hasPath(MAX_ADMISSIBLE_KEY) ?
      queueConfig.getInt(MAX_ADMISSIBLE_KEY) : MAX_ADMISSIBLE_DEFAULT;
    this.maxWaitingQuery = queueConfig.hasPath(MAX_WAITING_KEY) ?
      queueConfig.getInt(MAX_WAITING_KEY) : MAX_WAITING_DEFAULT;
    this.maxWaitingTimeout = queueConfig.hasPath(MAX_WAIT_TIMEOUT_KEY) ?
      queueConfig.getInt(MAX_WAIT_TIMEOUT_KEY) : RMCommonDefaults.MAX_WAIT_TIMEOUT_IN_MS_DEFAULT;
    this.waitForPreferredNodes = queueConfig.hasPath(WAIT_FOR_PREFERRED_NODES_KEY) ?
      queueConfig.getBoolean(WAIT_FOR_PREFERRED_NODES_KEY) : RMCommonDefaults.WAIT_FOR_PREFERRED_NODES_DEFAULT;
    this.queryPerNodeResourceShare = parseAndGetNodeShare(queueConfig);
  }

  @Override
  public String getQueueId() {
    return queueUUID;
  }

  @Override
  public String getQueueName() {
    return queueName;
  }

  /**
   * Total memory share of this queue in the cluster based on per node resource share. It assumes that the cluster is
   * made up of homogeneous nodes in terms of resources
   * @param numClusterNodes - total number of available cluster nodes which can participate in this queue
   * @return - queue memory share in MB
   */
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

  /**
   * Parses queues {@link QueryQueueConfigImpl#MAX_QUERY_MEMORY_PER_NODE_KEY} configuration to get memory value in
   * bytes. The format used for parsing the configuration value is defined by
   * {@link QueryQueueConfigImpl#MAX_QUERY_MEMORY_PER_NODE_FORMAT}
   * @param queueConfig - Queue Configuration object
   * @return - NodeResources created with value of {@link QueryQueueConfigImpl#MAX_QUERY_MEMORY_PER_NODE_KEY}
   * @throws RMConfigException - In case the config value is absent or doesn't follow the specified format
   */
  private NodeResources parseAndGetNodeShare(Config queueConfig) throws RMConfigException {
    try {
      String memoryPerNode = queueConfig.getString(MAX_QUERY_MEMORY_PER_NODE_KEY);
      Pattern pattern = Pattern.compile(MAX_QUERY_MEMORY_PER_NODE_FORMAT);
      Matcher patternMatcher = pattern.matcher(memoryPerNode);

      long memoryPerNodeInBytes = 0;
      if (patternMatcher.matches()) {
        memoryPerNodeInBytes = Long.parseLong(patternMatcher.group(1));

        // group 2 can be optional
        String group2 = patternMatcher.group(2);
        if (!group2.isEmpty()) {
          switch(group2.charAt(0)) {
            case 'G':
            case 'g':
              memoryPerNodeInBytes *= 1073741824L;
              break;
            case 'K':
            case 'k':
              memoryPerNodeInBytes *= 1024L;
              break;
            case 'M':
            case 'm':
              memoryPerNodeInBytes *= 1048576L;
              break;
            default:
              throw new RMConfigException(String.format("Configuration value of %s for queue %s didn't matched any of" +
                " supported suffixes. [Details: Supported: kKmMgG, Actual: %s",
                MAX_QUERY_MEMORY_PER_NODE_KEY, queueName, group2));
          }
        }
      } else {
        throw new RMConfigException(String.format("Configuration value of %s for queue %s didn't matched supported " +
          "format. Supported format is %s?", MAX_QUERY_MEMORY_PER_NODE_KEY, queueName, MAX_QUERY_MEMORY_PER_NODE_FORMAT));
      }

      return new NodeResources(memoryPerNodeInBytes, Integer.MAX_VALUE);
    } catch(Exception ex) {
      throw new RMConfigException(String.format("Failed while parsing %s for queue %s", MAX_QUERY_MEMORY_PER_NODE_KEY,
        queueName), ex);
    }
  }

  @Override
  public String toString() {
    return "{ QueueName: " + queueName + ", QueueId: " + queueUUID + ", QueuePerNodeResource(MB): " +
      queryPerNodeResourceShare.toString() + ", MaxQueryMemPerNode(MB): " + queryPerNodeResourceShare.toString() +
      ", MaxAdmissible: " + maxAdmissibleQuery + ", MaxWaiting: " + maxWaitingQuery + ", MaxWaitTimeout: " +
      maxWaitingTimeout + ", WaitForPreferredNodes: " + waitForPreferredNodes + "}";
  }
}
