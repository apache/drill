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

import java.util.List;

import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

public abstract class AbstractExchange extends AbstractSingle implements Exchange {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractExchange.class);

  protected int senderMajorFragmentId;
  protected int receiverMajorFragmentId;

  public AbstractExchange(PhysicalOperator child) {
    super(child);
  }

  /**
   * Exchanges are not executable. The Execution layer first has to set their parallelization and convert them into
   * something executable
   */
  @Override
  public boolean isExecutable() {
    return false;
  }

  @Override
  public int getMaxReceiveWidth() {
    return Integer.MAX_VALUE;
  }

  protected abstract void setupSenders(List<DrillbitEndpoint> senderLocations) throws PhysicalOperatorSetupException ;
  protected abstract void setupReceivers(List<DrillbitEndpoint> senderLocations) throws PhysicalOperatorSetupException ;

  @Override
  public final void setupSenders(int majorFragmentId, List<DrillbitEndpoint> senderLocations) throws PhysicalOperatorSetupException {
    this.senderMajorFragmentId = majorFragmentId;
    setupSenders(senderLocations);
  }


  @Override
  public final void setupReceivers(int majorFragmentId, List<DrillbitEndpoint> receiverLocations) throws PhysicalOperatorSetupException {
    this.receiverMajorFragmentId = majorFragmentId;
    setupReceivers(receiverLocations);
  }

  @Override
  public final <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitExchange(this, value);
  }

  @Override
  public int getOperatorType() {
    throw new UnsupportedOperationException();
  }


}
