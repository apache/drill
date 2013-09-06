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
package org.apache.drill.exec.pop;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.fragment.Fragment;
import org.apache.drill.exec.planner.fragment.PlanningSet;
import org.apache.drill.exec.planner.fragment.SimpleParallelizer;
import org.apache.drill.exec.planner.fragment.StatsCollector;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.work.QueryWorkUnit;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestFragmentChecker extends PopUnitTestBase{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestFragmentChecker.class);
  
  
  @Test
  public void checkSimpleExchangePlan() throws Exception{
    print("/physical_double_exchange.json", 2, 3);

  }
  
  
  private void print(String fragmentFile, int bitCount, int exepectedFragmentCount) throws Exception{
    
    System.out.println(String.format("=================Building plan fragments for [%s].  Allowing %d total Drillbits.==================", fragmentFile, bitCount));
    PhysicalPlanReader ppr = new PhysicalPlanReader(CONFIG, CONFIG.getMapper(), DrillbitEndpoint.getDefaultInstance());
    Fragment fragmentRoot = getRootFragment(ppr, fragmentFile);
    PlanningSet planningSet = StatsCollector.collectStats(fragmentRoot);
    SimpleParallelizer par = new SimpleParallelizer();
    List<DrillbitEndpoint> endpoints = Lists.newArrayList();
    DrillbitEndpoint localBit = null;
    for(int i =0; i < bitCount; i++){
      DrillbitEndpoint b1 = DrillbitEndpoint.newBuilder().setAddress("localhost").setBitPort(1234+i).build();
      if(i ==0) localBit = b1; 
      endpoints.add(b1);
    }
    
    
    QueryWorkUnit qwu = par.getFragments(localBit, QueryId.getDefaultInstance(), endpoints, ppr, fragmentRoot, planningSet, 10, 5);
    System.out.println(String.format("=========ROOT FRAGMENT [%d:%d] =========", qwu.getRootFragment().getHandle().getMajorFragmentId(), qwu.getRootFragment().getHandle().getMinorFragmentId()));
    
    System.out.print(qwu.getRootFragment().getFragmentJson());
    
    
    for(PlanFragment f : qwu.getFragments()){
      System.out.println(String.format("=========Fragment [%d:%d]=====", f.getHandle().getMajorFragmentId(), f.getHandle().getMinorFragmentId()));
      System.out.print(f.getFragmentJson());
    }
    
    //assertEquals(exepectedFragmentCount, qwu.getFragments().size());

    logger.debug("Planning Set {}", planningSet);
  }
  
  @Test
  public void validateSingleExchangeFragment() throws Exception{
    print("/physical_single_exchange.json", 1, 2);

  }
}
