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
package org.apache.drill.exec.physical;

import com.google.common.base.Preconditions;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.google.protobuf.TextFormat;

/**
 * EndpointAffinity captures affinity value for a given single Drillbit endpoint.
 */
public class EndpointAffinity {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EndpointAffinity.class);

  private final DrillbitEndpoint endpoint;
  private double affinity = 0.0d;

  /**
   * Create EndpointAffinity instance for given Drillbit endpoint. Affinity is initialized to 0. Affinity can be added
   * after EndpointAffinity object creation using {@link #addAffinity(double)}.
   *
   * @param endpoint Drillbit endpoint.
   */
  public EndpointAffinity(DrillbitEndpoint endpoint) {
    this.endpoint = endpoint;
  }

  /**
   * Create EndpointAffinity instance for given Drillbit endpoint and affinity initialized to given affinity value.
   * Affinity can be added after EndpointAffinity object creation using {@link #addAffinity(double)}.
   *
   * @param endpoint Drillbit endpoint.
   * @param affinity Initial affinity value.
   */
  public EndpointAffinity(DrillbitEndpoint endpoint, double affinity) {
    this.endpoint = endpoint;
    this.affinity = affinity;
  }

  /**
   * Return the Drillbit endpoint in this instance.
   *
   * @return Drillbit endpoint.
   */
  public DrillbitEndpoint getEndpoint() {
    return endpoint;
  }

  /**
   * Get the affinity value. Affinity value is Double.POSITIVE_INFINITY if the Drillbit endpoint requires an assignment.
   *
   * @return affinity value
   */
  public double getAffinity() {
    return affinity;
  }

  /**
   * Add given affinity value to existing affinity value.
   *
   * @param f Affinity value (must be a non-negative value).
   * @throws java.lang.IllegalArgumentException If the given affinity value is negative.
   */
  public void addAffinity(double f){
    Preconditions.checkArgument(f >= 0.0d, "Affinity should not be negative");
    if (Double.POSITIVE_INFINITY == f) {
      affinity = f;
    } else if (Double.POSITIVE_INFINITY != affinity) {
      affinity += f;
    }
  }

  /**
   * Is this endpoint required to be in fragment endpoint assignment list?
   *
   * @return Returns true for mandatory assignment, false otherwise.
   */
  public boolean isAssignmentRequired() {
    return Double.POSITIVE_INFINITY == affinity;
  }

  @Override
  public String toString() {
    return "EndpointAffinity [endpoint=" + TextFormat.shortDebugString(endpoint) + ", affinity=" + affinity + "]";
  }
}