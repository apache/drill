/*
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

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractPhysicalVisitor;
import org.apache.drill.exec.physical.base.Exchange;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Store;
import org.apache.drill.exec.physical.base.SubScan;

import com.google.common.collect.Lists;

public class Materializer extends AbstractPhysicalVisitor<PhysicalOperator, Materializer.IndexedFragmentNode, ExecutionSetupException>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Materializer.class);

  public static final Materializer INSTANCE = new Materializer();

  private Materializer() {
  }

  @Override
  public PhysicalOperator visitExchange(Exchange exchange, IndexedFragmentNode iNode) throws ExecutionSetupException {
    iNode.addAllocation(exchange);
    if(exchange == iNode.getNode().getSendingExchange()){

      // this is a sending exchange.
      PhysicalOperator child = exchange.getChild().accept(this, iNode);
      PhysicalOperator materializedSender = exchange.getSender(iNode.getMinorFragmentId(), child);
      materializedSender.setOperatorId(0);
      materializedSender.setCost(exchange.getCost());
//      logger.debug("Visit sending exchange, materialized {} with child {}.", materializedSender, child);
      return materializedSender;

    }else{
      // receiving exchange.
      PhysicalOperator materializedReceiver = exchange.getReceiver(iNode.getMinorFragmentId());
      materializedReceiver.setOperatorId(Short.MAX_VALUE & exchange.getOperatorId());
//      logger.debug("Visit receiving exchange, materialized receiver: {}.", materializedReceiver);
      materializedReceiver.setCost(exchange.getCost());
      return materializedReceiver;
    }
  }

  @Override
  public PhysicalOperator visitGroupScan(GroupScan groupScan, IndexedFragmentNode iNode) throws ExecutionSetupException {
    PhysicalOperator child = groupScan.getSpecificScan(iNode.getMinorFragmentId());
    child.setOperatorId(Short.MAX_VALUE & groupScan.getOperatorId());
    return child;
  }

  @Override
  public PhysicalOperator visitSubScan(SubScan subScan, IndexedFragmentNode value) throws ExecutionSetupException {
    value.addAllocation(subScan);
    // TODO - implement this
    return super.visitOp(subScan, value);
  }

  @Override
  public PhysicalOperator visitStore(Store store, IndexedFragmentNode iNode) throws ExecutionSetupException {
    PhysicalOperator child = store.getChild().accept(this, iNode);

    iNode.addAllocation(store);

    try {
      PhysicalOperator o = store.getSpecificStore(child, iNode.getMinorFragmentId());
      o.setOperatorId(Short.MAX_VALUE & store.getOperatorId());
//      logger.debug("New materialized store node {} with child {}", o, child);
      return o;
    } catch (PhysicalOperatorSetupException e) {
      throw new FragmentSetupException("Failure while generating a specific Store materialization.");
    }
  }

  @Override
  public PhysicalOperator visitOp(PhysicalOperator op, IndexedFragmentNode iNode) throws ExecutionSetupException {
    iNode.addAllocation(op);
//    logger.debug("Visiting catch all: {}", op);
    List<PhysicalOperator> children = Lists.newArrayList();
    for(PhysicalOperator child : op){
      children.add(child.accept(this, iNode));
    }
    PhysicalOperator newOp = op.getNewWithChildren(children);
    newOp.setCost(op.getCost());
    newOp.setOperatorId(Short.MAX_VALUE & op.getOperatorId());
    return newOp;
  }

  public static class IndexedFragmentNode{
    final Wrapper info;
    final int minorFragmentId;

    public IndexedFragmentNode(int minorFragmentId, Wrapper info) {
      super();
      this.info = info;
      this.minorFragmentId = minorFragmentId;
    }

    public Fragment getNode() {
      return info.getNode();
    }

    public int getMinorFragmentId() {
      return minorFragmentId;
    }

    public Wrapper getInfo() {
      return info;
    }

    public void addAllocation(PhysicalOperator pop) {
      info.addAllocation(pop);
    }

  }

}
