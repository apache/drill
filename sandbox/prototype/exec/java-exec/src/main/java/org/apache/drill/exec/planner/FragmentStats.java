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
package org.apache.drill.exec.planner;

import org.apache.drill.common.physical.OperatorCost;

public class FragmentStats {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentStats.class);
  
  private int maxWidth = Integer.MAX_VALUE;
  private float networkCost; 
  private float diskCost;
  private float memoryCost;
  private float cpuCost;
  
  public void addMaxWidth(int width){
    maxWidth = Math.min(maxWidth, width);
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
  
  
}
