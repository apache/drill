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
package org.apache.drill.common.physical.pop;

import org.apache.drill.common.config.CommonConstants;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.graph.GraphVisitor;
import org.apache.drill.common.physical.FieldSet;
import org.apache.drill.common.physical.POPCost;
import org.apache.drill.common.util.PathScanner;

import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class POPBase implements PhysicalOperator{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(POPBase.class);
  
  private FieldSet fieldSet;
  
  
  public POPBase(FieldSet fieldSet){
    this.fieldSet = fieldSet;
  }
  
  public synchronized static Class<?>[] getSubTypes(DrillConfig config){
    Class<?>[] ops = PathScanner.scanForImplementationsArr(PhysicalOperator.class, config.getStringList(CommonConstants.PHYSICAL_OPERATOR_SCAN_PACKAGES));
    logger.debug("Adding Physical Operator sub types: {}", ((Object) ops) );
    return ops;
  }
  
  @JsonProperty("fields")
  public FieldSet getFieldSet(){
    return fieldSet;
  }

  @Override
  public void accept(GraphVisitor<PhysicalOperator> visitor) {
    visitor.enter(this);
    if(this.iterator() == null) throw new IllegalArgumentException("Null iterator for pop." + this);
    for(PhysicalOperator o : this){
      o.accept(visitor);  
    }
    visitor.leave(this);
  }

  @Override
  public POPCost getCost() {
    return null;
  }
  
}
