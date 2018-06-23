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
package org.apache.drill.exec.physical.config;

import java.util.Collections;
import java.util.List;

import org.apache.drill.exec.physical.MinorFragmentEndpoint;
import org.apache.drill.exec.physical.base.AbstractSender;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("range-sender")
public class RangeSender extends AbstractSender{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RangeSender.class);

  List<EndpointPartition> partitions;

  @JsonCreator
  public RangeSender(@JsonProperty("receiver-major-fragment") int oppositeMajorFragmentId, @JsonProperty("child") PhysicalOperator child, @JsonProperty("partitions") List<EndpointPartition> partitions) {
    super(oppositeMajorFragmentId, child, Collections.<MinorFragmentEndpoint>emptyList());
    this.partitions = partitions;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new RangeSender(oppositeMajorFragmentId, child, partitions);
  }

  public static class EndpointPartition{
    private final PartitionRange range;
    private final DrillbitEndpoint endpoint;

    @JsonCreator
    public EndpointPartition(@JsonProperty("range") PartitionRange range, @JsonProperty("endpoint") DrillbitEndpoint endpoint) {
      super();
      this.range = range;
      this.endpoint = endpoint;
    }
    public PartitionRange getRange() {
      return range;
    }
    public DrillbitEndpoint getEndpoint() {
      return endpoint;
    }
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.RANGE_SENDER_VALUE;
  }
}
