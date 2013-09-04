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
package org.apache.drill.exec.physical.base;

import org.apache.drill.exec.physical.OperatorCost;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A container class that holds both send and receive costs for an exchange node.
 */
public class ExchangeCost {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExchangeCost.class);
  
  private final OperatorCost send;
  private final OperatorCost receive;
  private final OperatorCost combined;
  
  @JsonCreator
  public ExchangeCost(@JsonProperty("send") OperatorCost send, @JsonProperty("receive") OperatorCost receive) {
    this.send = send;
    this.receive = receive;
    this.combined =  OperatorCost.combine(send,  receive);
  }


  
  @JsonIgnore
  public OperatorCost getCombinedCost(){
    return combined;
  }

  @JsonProperty("send")
  public OperatorCost getSendCost() {
    return send;
  }

  @JsonProperty("receive")
  public OperatorCost getReceiveCost() {
    return receive;
  }
  
  public static ExchangeCost getSimpleEstimate(Size s){
    long cnt = s.getRecordCount();
    int sz = s.getRecordSize();
    OperatorCost send = new OperatorCost(cnt*sz, 0, 0, cnt);
    OperatorCost receive = new OperatorCost(cnt*sz, 0, 0, cnt);
    return new ExchangeCost(send, receive);
  }
  
}
