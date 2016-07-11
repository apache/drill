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

import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.physical.base.AbstractSingle;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;

@JsonTypeName("streaming-aggregate")
public class StreamingAggregate extends AbstractSingle {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StreamingAggregate.class);

  private final List<NamedExpression> keys;
  private final List<NamedExpression> exprs;

  private final float cardinality;

  @JsonCreator
  public StreamingAggregate(@JsonProperty("child") PhysicalOperator child, @JsonProperty("keys") List<NamedExpression> keys, @JsonProperty("exprs") List<NamedExpression> exprs, @JsonProperty("cardinality") float cardinality) {
    super(child);
    this.keys = keys;
    this.exprs = exprs;
    this.cardinality = cardinality;
  }

  public List<NamedExpression> getKeys() {
    return keys;
  }

  public List<NamedExpression> getExprs() {
    return exprs;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E{
    return physicalVisitor.visitStreamingAggregate(this, value);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new StreamingAggregate(child, keys, exprs, cardinality);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.STREAMING_AGGREGATE_VALUE;
  }

}
