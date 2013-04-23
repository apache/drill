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

import org.apache.drill.common.physical.pop.base.AbstractPhysicalVisitor;
import org.apache.drill.common.physical.pop.base.Exchange;
import org.apache.drill.common.physical.pop.base.PhysicalOperator;
import org.apache.drill.common.physical.pop.base.Scan;
import org.apache.drill.common.physical.pop.base.Store;
import org.apache.drill.exec.planner.FragmentNode.ExchangeFragmentPair;

import com.google.common.base.Preconditions;

public class FragmentStatsCollector implements FragmentVisitor<Void, FragmentStats> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentStatsCollector.class);

  //private HashMap<FragmentNode, FragmentStats> nodeStats = Maps.newHashMap();
  private final StatsCollector opCollector = new StatsCollector();
  private final FragmentPlanningSet planningSet;
  
  public FragmentStatsCollector(FragmentPlanningSet planningSet){
    this.planningSet = planningSet;
  }
  
  @Override
  public Void visit(FragmentNode n, FragmentStats stats) {
    Preconditions.checkNotNull(stats);
    Preconditions.checkNotNull(n);

    n.getRoot().accept(opCollector, stats);

    // sending exchange.
    Exchange sending = n.getSendingExchange();
    if (sending != null) {
      stats.addCost(sending.getAggregateSendCost());
      stats.addMaxWidth(sending.getMaxSendWidth());
    }

    // receivers...
    for (ExchangeFragmentPair child : n) {
      // add exchange receive cost.
      Exchange receivingExchange = child.getExchange();
      stats.addCost(receivingExchange.getAggregateReceiveCost());

      FragmentStats childStats = new FragmentStats();
      FragmentNode childNode = child.getNode();
      childNode.accept(this, childStats);
    }
    
    // store the stats for later use.
    planningSet.setStats(n, stats);
    
    return null;
  }

  public void collectStats(FragmentNode rootFragment) {
    FragmentStats s = new FragmentStats();
    rootFragment.accept(this, s);
  }

  private class StatsCollector extends AbstractPhysicalVisitor<Void, FragmentStats, RuntimeException> {

    @Override
    public Void visitExchange(Exchange exchange, FragmentStats stats) throws RuntimeException {
      // don't do anything here since we'll add the exchange costs elsewhere. We also don't want navigate across
      // exchanges since they are separate fragments.
      return null;
    }

    @Override
    public Void visitScan(Scan<?> scan, FragmentStats stats) {
      stats.addMaxWidth(scan.getReadEntries().size());
      return super.visitScan(scan, stats);
    }

    @Override
    public Void visitStore(Store store, FragmentStats stats) {
      stats.addMaxWidth(store.getMaxWidth());
      return super.visitStore(store, stats);
    }

    @Override
    public Void visitUnknown(PhysicalOperator op, FragmentStats stats) {
      stats.addCost(op.getCost());
      for (PhysicalOperator child : op) {
        child.accept(this, stats);
      }
      return null;
    }

  }

  
}
