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
package org.apache.drill.exec.physical.base;

import org.apache.drill.exec.physical.OperatorCost;

import com.fasterxml.jackson.annotation.JsonProperty;



public abstract class AbstractSender extends AbstractSingle implements Sender {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractSender.class);

  protected final int oppositeMajorFragmentId;
  
  public AbstractSender(int oppositeMajorFragmentId, PhysicalOperator child) {
    super(child);
    this.oppositeMajorFragmentId = oppositeMajorFragmentId;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSender(this, value);
  }

  @Override
  public OperatorCost getCost() {
    float network = child.getSize().getAggSize();
    return new OperatorCost(network, 0, 1000, child.getSize().getRecordCount());
  }

  @Override
  public int getOppositeMajorFragmentId() {
    return oppositeMajorFragmentId;
  }
  
  
}
