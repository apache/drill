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

import java.util.Iterator;
import java.util.Map;

import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.google.common.collect.Maps;

public class PlanningSet implements Iterable<Wrapper>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlanningSet.class);
  
  private Map<Fragment, Wrapper> fragmentMap = Maps.newHashMap();
  private int majorFragmentIdIndex = 0;
  
  PlanningSet(){
  }

  public void addAffinity(Fragment n, DrillbitEndpoint endpoint, float affinity){
    get(n).addEndpointAffinity(endpoint, affinity);
  }
  
  public void setWidth(Fragment n, int width){
    get(n).setWidth(width);
  }
  
  Wrapper get(Fragment node){
    Wrapper wrapper = fragmentMap.get(node);
    if(wrapper == null){
      wrapper = new Wrapper(node, majorFragmentIdIndex++);
      fragmentMap.put(node,  wrapper);
    }
    return wrapper;
  }

  @Override
  public Iterator<Wrapper> iterator() {
    return this.fragmentMap.values().iterator();
  }

  @Override
  public String toString() {
    return "FragmentPlanningSet:\n" + fragmentMap.values() + "]";
  }
  
  
  
}
