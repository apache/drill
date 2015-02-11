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
package org.apache.drill.exec.planner.fragment;


public class Stats {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Stats.class);

  private int maxWidth = Integer.MAX_VALUE;
  private double maxCost = 0.0;

  public void addMaxWidth(int maxWidth){
    this.maxWidth = Math.min(this.maxWidth, maxWidth);
  }

  public void addCost(double cost){
    maxCost = Math.max(maxCost, cost);
  }

  public int getMaxWidth() {
    return maxWidth;
  }

  @Override
  public String toString() {
    return "Stats [maxWidth=" + maxWidth + ", maxCost=" + maxCost + "]";
  }

  public double getMaxCost() {
    return maxCost;
  }

}
