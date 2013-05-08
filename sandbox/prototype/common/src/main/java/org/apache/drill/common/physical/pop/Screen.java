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

import java.util.Collections;
import java.util.List;

import org.apache.drill.common.physical.EndpointAffinity;
import org.apache.drill.common.physical.pop.base.AbstractStore;
import org.apache.drill.common.physical.pop.base.PhysicalOperator;
import org.apache.drill.common.physical.pop.base.Store;
import org.apache.drill.common.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("screen")
public class Screen extends AbstractStore {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Screen.class);

  private final DrillbitEndpoint endpoint;

  public Screen(@JsonProperty("child") PhysicalOperator child, @JacksonInject DrillbitEndpoint endpoint) {
    super(child);
    this.endpoint = endpoint;
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return Collections.singletonList(new EndpointAffinity(endpoint, 1000));
  }

  @Override
  public int getMaxWidth() {
    return 1;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) {
    // we actually don't have to do anything since nothing should have changed. we'll check just check that things
    // didn't get screwed up.
    if (endpoints.size() != 1)
      throw new UnsupportedOperationException("A Screen operator can only be assigned to a single node.");
    DrillbitEndpoint endpoint = endpoints.iterator().next();
    if (this.endpoint != endpoint)
      throw new UnsupportedOperationException("A Screen operator can only be assigned to its home node.");

  }

  @Override
  public Store getSpecificStore(PhysicalOperator child, int minorFragmentId) {
    return new Screen(child, endpoint);
  }

  @JsonIgnore
  public DrillbitEndpoint getEndpoint() {
    return endpoint;
  }

}
