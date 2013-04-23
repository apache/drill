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
import org.apache.drill.common.physical.pop.base.AbstractStore;
import org.apache.drill.common.physical.pop.base.Exchange;
import org.apache.drill.common.physical.pop.base.PhysicalOperator;
import org.apache.drill.common.physical.pop.base.Scan;
import org.apache.drill.common.physical.pop.base.Store;
import org.apache.drill.exec.exception.FragmentSetupException;

public class FragmentMaterializer extends AbstractPhysicalVisitor<PhysicalOperator, FragmentMaterializer.IndexedFragmentNode, FragmentSetupException>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentMaterializer.class);

  
  @Override
  public PhysicalOperator visitExchange(Exchange exchange, IndexedFragmentNode iNode) throws FragmentSetupException {
    if(exchange == iNode.getNode().getSendingExchange()){
      
      // this is a sending exchange.
      PhysicalOperator child = exchange.getChild();
      return exchange.getSender(iNode.getMinorFragmentId(), child);
      
    }else{
      // receiving exchange.
      return exchange.getReceiver(iNode.getMinorFragmentId());
    }
  }

  @Override
  public PhysicalOperator visitScan(Scan<?> scan, IndexedFragmentNode iNode) throws FragmentSetupException {
    return scan.getSpecificScan(iNode.getMinorFragmentId());
  }

  @Override
  public PhysicalOperator visitStore(Store store, IndexedFragmentNode iNode) throws FragmentSetupException {
    PhysicalOperator child = store.getChild();
    return store.getSpecificStore(child, iNode.getMinorFragmentId());
  }

  @Override
  public PhysicalOperator visitUnknown(PhysicalOperator op, IndexedFragmentNode iNode) throws FragmentSetupException {
    return op;
  }
  
  public static class IndexedFragmentNode{
    final FragmentWrapper info;
    final int minorFragmentId;
    
    public IndexedFragmentNode(int minorFragmentId, FragmentWrapper info) {
      super();
      this.info = info;
      this.minorFragmentId = minorFragmentId;
    }

    public FragmentNode getNode() {
      return info.getNode();
    }

    public int getMinorFragmentId() {
      return minorFragmentId;
    }

    public FragmentWrapper getInfo() {
      return info;
    }
    
  }
  
}
