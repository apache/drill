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
                              NodeResources queueNodeResource) throws RMConfigException {
    this.queueUUID = UUID.randomUUID().toString();
    this.queueName = poolName;
    parseQueueConfig(queueConfig, queueNodeResource);
  }

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

  private NodeResources parseAndGetNodeShare(Config queueConfig) throws RMConfigException {
    try {
      String memoryPerNode = queueConfig.getString(MAX_QUERY_MEMORY_PER_NODE_KEY);
      String memoryPattern = "([0-9]+)\\s*([kKmMgG]?)\\s*$";
      Pattern pattern = Pattern.compile(memoryPattern);
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
          "format. Supported format is [0-9]*[kKmMgG]?", MAX_QUERY_MEMORY_PER_NODE_KEY, queueName));
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
