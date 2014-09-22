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

import org.apache.drill.exec.physical.base.Exchange;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.HasAffinity;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Store;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.physical.config.Limit;
import org.apache.drill.exec.physical.config.WindowPOP;
import org.apache.drill.exec.planner.AbstractOpWrapperVisitor;
import org.apache.drill.exec.planner.fragment.Fragment.ExchangeFragmentPair;

import com.google.common.base.Preconditions;

public class StatsCollector {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StatsCollector.class);

  private final static OpStatsCollector opStatCollector = new OpStatsCollector();

  private StatsCollector() {
  };

  private static void visit(PlanningSet planningSet, Fragment n) {
    Preconditions.checkNotNull(planningSet);
    Preconditions.checkNotNull(n);

    Wrapper wrapper = planningSet.get(n);
    n.getRoot().accept(opStatCollector, wrapper);
//    logger.debug("Set stats to {}", wrapper.getStats());
    // receivers...
    for (ExchangeFragmentPair child : n) {
      // get the fragment node that feeds this node.
      Fragment childNode = child.getNode();
      visit(planningSet, childNode);
    }

  }

  public static PlanningSet collectStats(Fragment rootFragment) {
    PlanningSet fps = new PlanningSet();
    visit(fps, rootFragment);
    return fps;
  }

  private static class OpStatsCollector extends AbstractOpWrapperVisitor<Void, RuntimeException> {

    @Override
    public Void visitSendingExchange(Exchange exchange, Wrapper wrapper) throws RuntimeException {
      Stats stats = wrapper.getStats();
      stats.addMaxWidth(exchange.getMaxSendWidth());
      return super.visitSendingExchange(exchange, wrapper);
    }

    @Override
    public Void visitReceivingExchange(Exchange exchange, Wrapper wrapper) throws RuntimeException {
      // no traversal since it would cross fragment boundary.
      return null;
    }

    @Override
    public Void visitGroupScan(GroupScan groupScan, Wrapper wrapper) {
      Stats stats = wrapper.getStats();
      stats.addMaxWidth(groupScan.getMaxParallelizationWidth());
      return super.visitGroupScan(groupScan, wrapper);
    }

    @Override
    public Void visitSubScan(SubScan subScan, Wrapper wrapper) throws RuntimeException {
      // TODO - implement this
      return visitOp(subScan, wrapper);
    }

    @Override
    public Void visitStore(Store store, Wrapper wrapper) {
      Stats stats = wrapper.getStats();
      stats.addMaxWidth(store.getMaxWidth());
      return super.visitStore(store, wrapper);
    }

    @Override
    public Void visitLimit(Limit limit, Wrapper wrapper) throws RuntimeException {
      // TODO: Implement this
      return visitOp(limit, wrapper);
    }

    @Override
    public Void visitOp(PhysicalOperator op, Wrapper wrapper) {
      if(op instanceof HasAffinity){
        wrapper.addEndpointAffinity(((HasAffinity)op).getOperatorAffinity());
      }
      Stats stats = wrapper.getStats();
      stats.addCost(op.getCost());
      for (PhysicalOperator child : op) {
        child.accept(this, wrapper);
      }
      return null;
    }

    @Override
    public Void visitWindowFrame(WindowPOP window, Wrapper value) throws RuntimeException {
      return visitOp(window, value);
    }

  }

}
