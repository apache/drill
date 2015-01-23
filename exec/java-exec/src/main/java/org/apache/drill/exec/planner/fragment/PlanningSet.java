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
package org.apache.drill.exec.planner.fragment;

import java.util.Iterator;
import java.util.Map;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.google.common.collect.Maps;

public class PlanningSet implements Iterable<Wrapper> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlanningSet.class);

  private final Map<Fragment, Wrapper> fragmentMap = Maps.newHashMap();
  private int majorFragmentIdIndex = 0;

  public Wrapper get(Fragment node) {
    Wrapper wrapper = fragmentMap.get(node);
    if (wrapper == null) {

      int majorFragmentId = 0;

      // If there is a sending exchange, we need to number other than zero.
      if (node.getSendingExchange() != null) {

        // assign the upper 16 bits as the major fragment id.
        majorFragmentId = node.getSendingExchange().getChild().getOperatorId() >> 16;

        // if they are not assigned, that means we mostly likely have an externally generated plan.  in this case, come up with a major fragmentid.
        if (majorFragmentId == 0) {
          majorFragmentId = majorFragmentIdIndex;
        }
      }
      wrapper = new Wrapper(node, majorFragmentId);
      fragmentMap.put(node, wrapper);
      majorFragmentIdIndex++;
    }
    return wrapper;
  }

  @Override
  public Iterator<Wrapper> iterator() {
    return this.fragmentMap.values().iterator();
  }

  @Override
  public String toString() {
    return "FragmentPlanningSet:\n" + fragmentMap.values() + "]";
  }

}
