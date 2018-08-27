/*
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
import java.util.Set;

import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.physical.MinorFragmentEndpoint;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

public class PhysicalOperatorUtil {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PhysicalOperatorUtil.class);


  private PhysicalOperatorUtil() {}

  public static Set<Class<? extends PhysicalOperator>> getSubTypes(ScanResult classpathScan) {
    final Set<Class<? extends PhysicalOperator>> ops = classpathScan.getImplementations(PhysicalOperator.class);
    logger.debug("Found {} physical operator classes: {}.", ops.size(),
                 ops);
    return ops;
  }

  /**
   * Helper method to create a list of MinorFragmentEndpoint instances from a given endpoint assignment list.
   *
   * @param endpoints Assigned endpoint list. Index of each endpoint in list indicates the MinorFragmentId of the
   *                  fragment that is assigned to the endpoint.
   * @return
   */
  public static List<MinorFragmentEndpoint> getIndexOrderedEndpoints(List<DrillbitEndpoint> endpoints) {
    List<MinorFragmentEndpoint> destinations = Lists.newArrayList();
    int minorFragmentId = 0;
    for(DrillbitEndpoint endpoint : endpoints) {
      destinations.add(new MinorFragmentEndpoint(minorFragmentId, endpoint));
      minorFragmentId++;
    }

    return destinations;
  }
}
