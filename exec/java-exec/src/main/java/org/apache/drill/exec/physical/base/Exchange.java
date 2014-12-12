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

import com.fasterxml.jackson.annotation.JsonIgnore;

public interface Exchange extends PhysicalOperator {

  /**
   * Inform this Exchange node about its sender locations. This list should be index-ordered the same as the expected
   * minorFragmentIds for each sender.
   *
   * @param senderLocations
   */
  public abstract void setupSenders(int majorFragmentId, List<DrillbitEndpoint> senderLocations) throws PhysicalOperatorSetupException;

  /**
   * Inform this Exchange node about its receiver locations. This list should be index-ordered the same as the expected
   * minorFragmentIds for each receiver.
   *
   * @param receiverLocations
   */
  public abstract void setupReceivers(int majorFragmentId, List<DrillbitEndpoint> receiverLocations) throws PhysicalOperatorSetupException;

  /**
   * Get the Sender associated with the given minorFragmentId. Cannot be called until after setupSenders() and
   * setupReceivers() have been called.
   *
   * @param minorFragmentId
   *          The minor fragment id, must be in the range [0, fragment.width).
   * @param child
   *          The feeding node for the requested sender.
   * @return The materialized sender for the given arguments.
   */
  public abstract Sender getSender(int minorFragmentId, PhysicalOperator child) throws PhysicalOperatorSetupException;

  /**
   * Get the Receiver associated with the given minorFragmentId. Cannot be called until after setupSenders() and
   * setupReceivers() have been called.
   *
   * @param minorFragmentId
   *          The minor fragment id, must be in the range [0, fragment.width).
   * @return The materialized recevier for the given arguments.
   */
  public abstract Receiver getReceiver(int minorFragmentId);

  /**
   * The widest width this sender can send (max sending parallelization). Typically Integer.MAX_VALUE.
   *
   * @return
   */
  @JsonIgnore
  public abstract int getMaxSendWidth();

  /**
   * The widest width this receiver can receive(max receive parallelization). Default is Integer.MAX_VALUE.
   *
   * @return
   */
  @JsonIgnore
  public abstract int getMaxReceiveWidth();

  /**
   * Return the feeding child of this operator node.
   *
   * @return
   */
  public PhysicalOperator getChild();

}