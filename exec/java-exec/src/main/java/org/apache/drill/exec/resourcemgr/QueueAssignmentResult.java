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

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

public class QueueAssignmentResult {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueueAssignmentResult.class);

  private List<ResourcePool> selectedLeafPools = new ArrayList<>();

  private List<ResourcePool> rejectedPools = new ArrayList<>();

  public void addSelectedPool(ResourcePool pool) {
    Preconditions.checkState(pool.isLeafPool(), "Selected pool %s is not a leaf pool",
      pool.getPoolName());
    selectedLeafPools.add(pool);
  }

  public void addRejectedPool(ResourcePool pool) {
    rejectedPools.add(pool);
  }

  public List<ResourcePool> getSelectedLeafPools() {
    return selectedLeafPools;
  }

  public List<ResourcePool> getRejectedPools() {
    return rejectedPools;
  }

  public void logAssignmentResult(String queryId) {
    logger.debug("For query {}, list of selected leaf pools are {} and rejected pools are {}",
      queryId, selectedLeafPools, rejectedPools);
  }
}
