/**
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
package org.apache.drill.exec.planner.physical;

import org.eigenbase.relopt.RelOptCluster;

public class PlanningSettings {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlanningSettings.class);

  private static ThreadLocal<PlanningSettings> settings = new ThreadLocal<>();

  private boolean singleMode;

  public boolean isSingleMode() {
    return singleMode;
  }

  public void setSingleMode(boolean singleMode) {
    this.singleMode = singleMode;
  }

  /**
   * Convenience method to extract planning settings from RelOptCluster. Uses threadlocal until Optiq supports
   * passthrough.
   */
  public static PlanningSettings get(RelOptCluster cluster) {
    PlanningSettings s = settings.get();
    if (s == null) {
      s = new PlanningSettings();
      settings.set(s);
    }

    return s;
  }


  public static PlanningSettings get(){
    return get(null);
  }


}
