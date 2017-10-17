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
package org.apache.drill.exec.planner;

import java.util.Collection;

import org.apache.calcite.plan.RelOptPlanner;

/**
 * A callback that StoragePlugins can initialize to allow further configuration
 * of the Planner at initialization time. Examples could be to allow adding lattices,
 * materializations or additional traits to the planner that will be used in
 * planning.
 */
public abstract class PlannerCallback {

  /**
   * Method that will be called before a planner is used to further configure the planner.
   * @param planner The planner to be configured.
   */
  public abstract void initializePlanner(RelOptPlanner planner);


  public static PlannerCallback merge(Collection<PlannerCallback> callbacks){
    return new PlannerCallbackCollection(callbacks);
  }

  private static class PlannerCallbackCollection extends PlannerCallback{
    private Collection<PlannerCallback> callbacks;

    private PlannerCallbackCollection(Collection<PlannerCallback> callbacks){
      this.callbacks = callbacks;
    }

    @Override
    public void initializePlanner(RelOptPlanner planner) {
      for(PlannerCallback p : callbacks){
        p.initializePlanner(planner);
      }
    }


  }
}
