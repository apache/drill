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

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.PhysicalOperatorSetupException;
import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.physical.base.AbstractPhysicalVisitor;
import org.apache.drill.exec.physical.base.Exchange;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Scan;
import org.apache.drill.exec.physical.base.Store;

import com.google.common.collect.Lists;

public class Materializer extends AbstractPhysicalVisitor<PhysicalOperator, Materializer.IndexedFragmentNode, ExecutionSetupException>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Materializer.class);

  
  @Override
  public PhysicalOperator visitExchange(Exchange exchange, IndexedFragmentNode iNode) throws ExecutionSetupException {
    if(exchange == iNode.getNode().getSendingExchange()){
      
      // this is a sending exchange.
      PhysicalOperator child = exchange.getChild().accept(this, iNode);
      PhysicalOperator materializedSender = exchange.getSender(iNode.getMinorFragmentId(), child);
      logger.debug("Visit sending exchange, materialized {} with child {}.", materializedSender, child);
      return materializedSender;
      
    }else{
      // receiving exchange.
      PhysicalOperator materializedReceiver = exchange.getReceiver(iNode.getMinorFragmentId());
      logger.debug("Visit receiving exchange, materialized receiver: {}.", materializedReceiver);
      return materializedReceiver;
    }
  }

  @Override
  public PhysicalOperator visitScan(Scan<?> scan, IndexedFragmentNode iNode) throws ExecutionSetupException {
    return scan.getSpecificScan(iNode.getMinorFragmentId());
  }

  @Override
  public PhysicalOperator visitStore(Store store, IndexedFragmentNode iNode) throws ExecutionSetupException {
    PhysicalOperator child = store.getChild().accept(this, iNode);
    
    try {
      PhysicalOperator o = store.getSpecificStore(child, iNode.getMinorFragmentId());
      logger.debug("New materialized store node {} with child {}", o, child);
      return o;
    } catch (PhysicalOperatorSetupException e) {
      throw new FragmentSetupException("Failure while generating a specific Store materialization.");
    }
  }

  @Override
  public PhysicalOperator visitOp(PhysicalOperator op, IndexedFragmentNode iNode) throws ExecutionSetupException {
    logger.debug("Visiting catch all: {}", op);
    List<PhysicalOperator> children = Lists.newArrayList();
    for(PhysicalOperator child : op){
      children.add(child.accept(this, iNode));
    }
    return op.getNewWithChildren(children);
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
    
  }
  
}
