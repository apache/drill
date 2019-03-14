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
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.fragment.QueryParallelizer;
import org.apache.drill.exec.planner.fragment.DistributedQueueParallelizer;
import org.apache.drill.exec.resourcemgr.config.ResourcePoolTree;
import org.apache.drill.exec.resourcemgr.config.ResourcePoolTreeImpl;
import org.apache.drill.exec.resourcemgr.config.exception.RMConfigException;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.work.foreman.Foreman;

public class DistributedResourceManager implements ResourceManager {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DistributedResourceManager.class);

  private final ResourcePoolTree rmPoolTree;

  private final DrillbitContext context;

  private final DrillConfig rmConfig;
  public final long memoryPerNode;
  public final int cpusPerNode;

  public DistributedResourceManager(DrillbitContext context) throws DrillRuntimeException {
    memoryPerNode = DrillConfig.getMaxDirectMemory();
    cpusPerNode = Runtime.getRuntime().availableProcessors();
    try {
      this.context = context;
      this.rmConfig = DrillConfig.createForRM();
      rmPoolTree = new ResourcePoolTreeImpl(rmConfig, DrillConfig.getMaxDirectMemory(),
        Runtime.getRuntime().availableProcessors(), 1);
      logger.debug("Successfully parsed RM config \n{}", rmConfig.getConfig(ResourcePoolTreeImpl.ROOT_POOL_CONFIG_KEY));
    } catch (RMConfigException ex) {
      throw new DrillRuntimeException(String.format("Failed while parsing Drill RM Configs. Drillbit won't be started" +
        " unless config is fixed or RM is disabled by setting %s to false", ExecConstants.RM_ENABLED), ex);
    }
  }
  @Override
  public long memoryPerNode() {
    return memoryPerNode;
  }

  @Override
  public int cpusPerNode() {
    return cpusPerNode;
  }

  @Override
  public QueryResourceManager newQueryRM(Foreman foreman) {
    return new QueuedQueryResourceManager(this, foreman);
  }

  public ResourcePoolTree getRmPoolTree() {
    return rmPoolTree;
  }


  /**
   * Per-query resource manager. Handles resources and optional queue lease for
   * a single query. As such, this is a non-shared resource: it is associated
   * with a Foreman: a single thread at plan time, and a single event (in some
   * thread) at query completion time. Because of these semantics, no
   * synchronization is needed within this class.
   */

  public static class QueuedQueryResourceManager implements QueryResourceManager {

    private final Foreman foreman;
    private final QueryContext queryContext;
    private double queryCost;
    private final DistributedResourceManager rm;

    public QueuedQueryResourceManager(final DistributedResourceManager rm,
                                      final Foreman foreman) {
      this.foreman = foreman;
      this.queryContext = foreman.getQueryContext();
      this.rm = rm;
    }

    @Override
    public void setCost(double cost) {
      this.queryCost = cost;
    }

    @Override
    public QueryParallelizer getParallelizer(boolean planHasMemory) {
      // currently memory planning is disabled. Enable it once the RM functionality is fully implemented.
      return new DistributedQueueParallelizer(true || planHasMemory, this.queryContext);
    }

    @Override
    public void admit() throws QueryQueue.QueueTimeoutException, QueryQueue.QueryQueueException {
    }

    public long queryMemoryPerNode() {
      return 0;
    }

    @Override
    public long minimumOperatorMemory() {
      return 0;
    }

    @Override
    public void exit() {
    }

    @Override
    public boolean hasQueue() { return true; }

    @Override
    public String queueName() {
      return "";
    }
  }

  @Override
  public void close() {
  }
}
