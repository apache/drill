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
import org.apache.drill.exec.exception.FragmentSetupException;

/**
 * Responsible for breaking a plan into its constituent Fragments.
 */
public class FragmentingPhysicalVisitor extends AbstractPhysicalVisitor<FragmentNode, FragmentNode, FragmentSetupException> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentingPhysicalVisitor.class);

  private FragmentNode rootFragment = new FragmentNode();
  
  public FragmentingPhysicalVisitor(){
  }
  
  
  @Override
  public FragmentNode visitExchange(Exchange exchange, FragmentNode value) throws FragmentSetupException {
//    logger.debug("Visiting Exchange {}", exchange);
    if(value == null) throw new FragmentSetupException("The simple fragmenter was called without a FragmentBuilder value.  This will only happen if the initial call to SimpleFragmenter is by a Exchange node.  This should never happen since an Exchange node should never be the root node of a plan.");
    FragmentNode next = getNextBuilder();
    value.addReceiveExchange(exchange, next);
    next.addSendExchange(exchange);
    exchange.getChild().accept(this, getNextBuilder());
    return value;
  }
  
  @Override
  public FragmentNode visitUnknown(PhysicalOperator op, FragmentNode value)  throws FragmentSetupException{
//    logger.debug("Visiting Other {}", op);
    value = ensureBuilder(value);
    value.addOperator(op);
    for(PhysicalOperator child : op){
      child.accept(this, value);
    }
    return value;
  }
  
  private FragmentNode ensureBuilder(FragmentNode value) throws FragmentSetupException{
    if(value != null){
      return value;
    }else{
      return rootFragment;
    }
  }
  
  public FragmentNode getNextBuilder(){
    return new FragmentNode();
  }

}
