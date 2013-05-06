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
package org.apache.drill.exec.pop;

import static org.junit.Assert.*;

import java.util.Collection;
import java.util.Collections;

import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.fragment.Fragment;
import org.apache.drill.exec.planner.fragment.PlanningSet;
import org.apache.drill.exec.planner.fragment.StatsCollector;
import org.apache.drill.exec.planner.fragment.SimpleParallelizer;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.work.QueryWorkUnit;
import org.junit.Test;

import com.google.common.collect.Lists;

public class FragmentChecker extends PopUnitTestBase{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentChecker.class);
  
  
  @Test
  public void checkSimpleExchangePlan() throws Exception{
    
    PhysicalPlanReader ppr = new PhysicalPlanReader(CONFIG, CONFIG.getMapper(), DrillbitEndpoint.getDefaultInstance());
    Fragment fragmentRoot = getRootFragment(ppr, "/physical_simpleexchange.json");
    PlanningSet planningSet = StatsCollector.collectStats(fragmentRoot);
    SimpleParallelizer par = new SimpleParallelizer();
    
    DrillbitEndpoint b1 = DrillbitEndpoint.newBuilder().setAddress("localhost").setBitPort(1234).build();
    DrillbitEndpoint b2 = DrillbitEndpoint.newBuilder().setAddress("localhost").setBitPort(2345).build();
    
    QueryWorkUnit qwu = par.getFragments(b1, QueryId.getDefaultInstance(), Lists.newArrayList(b1, b2), ppr, fragmentRoot, planningSet, 10);
    assertEquals(qwu.getFragments().size(), 3);
    System.out.println("=========ROOT FRAGMENT=========");
    System.out.print(qwu.getRootFragment().getFragmentJson());
    
    
    for(PlanFragment f : qwu.getFragments()){
      System.out.println("=========");
      System.out.print(f.getFragmentJson());
    }
    logger.debug("Planning Set {}", planningSet);

  }
}
