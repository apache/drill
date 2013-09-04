/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.planner.fragment;

import org.apache.drill.exec.physical.OperatorCost;

public class Stats {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Stats.class);
  
  private int maxWidth = Integer.MAX_VALUE;
  private float networkCost; 
  private float diskCost;
  private float memoryCost;
  private float cpuCost;
  
  public void addMaxWidth(int maxWidth){
    this.maxWidth = Math.min(this.maxWidth, maxWidth);
  }
  
  public void addCost(OperatorCost cost){
    networkCost += cost.getNetwork();
    diskCost += cost.getDisk();
    memoryCost += cost.getMemory();
    cpuCost += cost.getCpu();
  }

  public int getMaxWidth() {
    return maxWidth;
  }

  public float getNetworkCost() {
    return networkCost;
  }

  public float getDiskCost() {
    return diskCost;
  }

  public float getMemoryCost() {
    return memoryCost;
  }

  public float getCpuCost() {
    return cpuCost;
  }

  @Override
  public String toString() {
    return "FragmentStats [maxWidth=" + maxWidth + ", networkCost=" + networkCost + ", diskCost=" + diskCost
        + ", memoryCost=" + memoryCost + ", cpuCost=" + cpuCost + "]";
  }
  
  
  
}
