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

import java.util.List;

import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("external-sort")
public class ExternalSort extends Sort {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExternalSort.class);

  @JsonCreator
  public ExternalSort(@JsonProperty("child") PhysicalOperator child, @JsonProperty("orderings") List<Ordering> orderings, @JsonProperty("reverse") boolean reverse) {
    super(child, orderings, reverse);
    initialAllocation = 20_000_000;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    ExternalSort newSort = new ExternalSort(child, orderings, reverse);
    newSort.setMaxAllocation(getMaxAllocation());
    return newSort;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.EXTERNAL_SORT_VALUE;
  }

  // Set here, rather than the base class, because this is the only
  // operator, at present, that makes use of the maximum allocation.
  // Remove this, in favor of the base class version, when Drill
  // sets the memory allocation for all operators.

  public void setMaxAllocation(long maxAllocation) {
    this.maxAllocation = maxAllocation;
  }
}
