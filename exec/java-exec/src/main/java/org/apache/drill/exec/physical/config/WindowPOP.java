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

package org.apache.drill.exec.physical.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.physical.base.AbstractSingle;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.proto.UserBitShared;

@JsonTypeName("window")
public class WindowPOP extends AbstractSingle {

  private final NamedExpression[] withins;
  private final NamedExpression[] aggregations;
  private final long start;
  private final long end;

  public WindowPOP(@JsonProperty("child") PhysicalOperator child,
                   @JsonProperty("within") NamedExpression[] withins,
                   @JsonProperty("aggregations") NamedExpression[] aggregations,
                   @JsonProperty("start") long start,
                   @JsonProperty("end") long end) {
    super(child);
    this.withins = withins;
    this.aggregations = aggregations;
    this.start = start;
    this.end = end;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new WindowPOP(child, withins, aggregations, start, end);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitWindowFrame(this, value);
  }

  @Override
  public int getOperatorType() {
    return UserBitShared.CoreOperatorType.WINDOW_VALUE;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public NamedExpression[] getAggregations() {
    return aggregations;
  }

  public NamedExpression[] getWithins() {
    return withins;
  }
}
