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
package org.apache.drill.exec.work.foreman.rm;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.local.LocalClusterCoordinator;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.SystemOptionManager;

/**
 * Builds the proper resource manager and queue implementation for the configured
 * system options.
 * <p>
 * <ul>
 * <li>If the Drillbit is embedded<ul>
 * <li>If queues are enabled, then the admission-controlled resource manager
 * with the local query queue.</li>
 * <li>Otherwise, the default resource manager and no queues.</li>
 * </ul></li>
 * <li>If the Drillbit is in a cluster<ul>
 * <li>If queues are enabled, then the admission-controlled resource manager
 * with the distributed query queue.</li>
 * <li>Otherwise, the default resource manager and no queues.</li>
 * </ul></li>
 * </ul>
 * Configuration settings:
 * <dl>
 * <dt>Cluster coordinator instance</dt>
 * <dd>If an instance of <tt>LocalClusterCoordinator</tt>, the Drillbit is
 * embedded, else it is in a cluster.</dd>
 * <dt><tt>drill.exec.queue.embedded.enable</tt> boot config<dt>
 * <dd>If enabled, and if embedded, then use the local queue.</dd>
 * <dt><tt>exec.queue.enable</tt> system option</dt>
 * <dd>If enabled, and if in a cluster, then use the distributed queue.</dd>
 * </dl>
 */
public class ResourceManagerBuilder {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ResourceManagerBuilder.class);

  private DrillbitContext context;

  public ResourceManagerBuilder(final DrillbitContext context) {
    this.context = context;
  }

  public ResourceManager build() {
    final ClusterCoordinator coord = context.getClusterCoordinator();
    final DrillConfig config = context.getConfig();
    final SystemOptionManager systemOptions = context.getOptionManager();
    if (coord instanceof LocalClusterCoordinator) {
      if (config.getBoolean(EmbeddedQueryQueue.ENABLED)) {
        logger.info("Enabling embedded, local query queue");
        return new ThrottledResourceManager(context, new EmbeddedQueryQueue(context));
      } else {
        logger.info("Zookeeper is not configured as ClusterCoordinator hence using Default Manager. [Details: " +
          "isRMEnabled: {}]", config.getBoolean(ExecConstants.RM_ENABLED));
        return new DefaultResourceManager();
      }
    } else if (config.getBoolean(ExecConstants.RM_ENABLED) && !systemOptions.getOption(ExecConstants.ENABLE_QUEUE)){
      logger.info("RM is enabled and queues are disabled so using Distributed Resource Manager");
      return new DistributedResourceManager(context);
    } else {
      logger.info("Using Dynamic Resource Manager to either enable Default of Throttled Resource Manager");
      return new DynamicResourceManager(context);
    }
  }
}
