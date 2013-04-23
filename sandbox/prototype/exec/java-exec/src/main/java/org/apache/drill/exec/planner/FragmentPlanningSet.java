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

import java.util.Iterator;
import java.util.Map;

import org.apache.drill.common.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.ops.QueryContext;

public class FragmentPlanningSet implements Iterable<FragmentWrapper>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentPlanningSet.class);
  
  private Map<FragmentNode, FragmentWrapper> fragmentMap;
  private int majorFragmentIdIndex = 0;
  private QueryContext context;
  
  public FragmentPlanningSet(QueryContext context){
    this.context = context;
  }
  
  public void setStats(FragmentNode node, FragmentStats stats){
    get(node).setStats(stats);
  }

  public void addAffinity(FragmentNode n, DrillbitEndpoint endpoint, float affinity){
    get(n).addEndpointAffinity(endpoint, affinity);
  }
  
  public void setWidth(FragmentNode n, int width){
    get(n).setWidth(width);
  }
  
  private FragmentWrapper get(FragmentNode node){
    FragmentWrapper info = fragmentMap.get(node);
    if(info == null) info = new FragmentWrapper(node, majorFragmentIdIndex++);
    return info;
  }

  @Override
  public Iterator<FragmentWrapper> iterator() {
    return this.fragmentMap.values().iterator();
  }
  
  
}
