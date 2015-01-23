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
import org.apache.drill.exec.planner.fragment.ParallelizationInfo;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JsonIgnore;

public interface Exchange extends PhysicalOperator {

  /**
   * Exchanges are fragment boundaries in physical operator tree. It is divided into two parts. First part is Sender
   * which becomes part of the sending fragment. Second part is Receiver which becomes part of the fragment that
   * receives the data.
   *
   * Assignment dependency describes whether sender fragments depend on receiver fragment's endpoint assignment for
   * determining its parallelization and endpoint assignment and vice versa.
   */
  public enum ParallelizationDependency {
    SENDER_DEPENDS_ON_RECEIVER, // Sending fragment depends on receiving fragment for parallelization
    RECEIVER_DEPENDS_ON_SENDER, // Receiving fragment depends on sending fragment for parallelization (default value).
  }

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
   * Provide parallelization parameters for sender side of the exchange. Output includes min width,
   * max width and affinity to Drillbits.
   *
   * @param receiverFragmentEndpoints Endpoints assigned to receiver fragment if available, otherwise an empty list.
   * @return
   */
  @JsonIgnore
  public abstract ParallelizationInfo getSenderParallelizationInfo(List<DrillbitEndpoint> receiverFragmentEndpoints);

  /**
   * Provide parallelization parameters for receiver side of the exchange. Output includes min width,
   * max width and affinity to Drillbits.
   *
   * @param senderFragmentEndpoints Endpoints assigned to receiver fragment if available, otherwise an empty list
   * @return
   */
  @JsonIgnore
  public abstract ParallelizationInfo getReceiverParallelizationInfo(List<DrillbitEndpoint> senderFragmentEndpoints);

  /**
   * Return the feeding child of this operator node.
   *
   * @return
   */
  public PhysicalOperator getChild();

  /**
   * Get the parallelization dependency of the Exchange.
   */
  @JsonIgnore
  public ParallelizationDependency getParallelizationDependency();
}