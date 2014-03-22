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
package org.apache.drill.exec.physical;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class OperatorCost {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OperatorCost.class);
  
  private final float network; 
  private final float disk;
  private final float memory;
  private final float cpu;
  
  
  
  @JsonCreator
  public OperatorCost(@JsonProperty("network") float network, @JsonProperty("disk") float disk, @JsonProperty("memory") float memory, @JsonProperty("cpu") float cpu) {
    super();
    this.network = network;
    this.disk = disk;
    this.memory = memory;
    this.cpu = cpu;
  }

  public float getNetwork() {
    return network;
  }

  public float getDisk() {
    return disk;
  }

  public float getMemory() {
    return memory;
  }

  public float getCpu() {
    return cpu;
  }
  
  public static OperatorCost combine(OperatorCost c1, OperatorCost c2){
    return new OperatorCost(c1.network + c2.network, c1.disk + c2.disk, c1.memory + c2.memory, c1.cpu + c2.cpu);
  }

  public OperatorCost add(OperatorCost c2){
    return combine(this, c2);
  }
  
  public int compare(OperatorCost c2) {
	float thisTotal = this.network + this.disk + this.memory + this.cpu;
	float c2Total   = c2.network + c2.disk + c2.memory + c2.cpu;
	if (thisTotal < c2Total) return -1;
	else if (thisTotal == c2Total) return 0;
	else return 1;
  } 
  
}
