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
import org.apache.drill.common.physical.pop.base.Store;

public class ScanFinder extends AbstractPhysicalVisitor<Boolean, Void, RuntimeException> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanFinder.class);

  private static final ScanFinder finder = new ScanFinder();
  
  private ScanFinder(){}
  
  @Override
  public Boolean visitExchange(Exchange exchange, Void value) throws RuntimeException {
    return false;
  }

  @Override
  public Boolean visitStore(Store store, Void value) throws RuntimeException {
    return true;
  }

  @Override
  public Boolean visitUnknown(PhysicalOperator op, Void value) throws RuntimeException {
    for(PhysicalOperator child : op){
      if(child.accept(this,  null)) return true;
    }
    return false;
  }
  
  public static boolean containsScan(PhysicalOperator op){
    return op.accept(finder, null);
  }
  
}
