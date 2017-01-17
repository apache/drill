/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import org.apache.drill.exec.physical.base.AbstractSingle;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.planner.physical.StatsMergePrel.OperatorPhase;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;

import java.util.List;

@JsonTypeName("statistics-merge")
public class StatisticsMerge extends AbstractSingle {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StatisticsAggregate.class);
  protected OperatorPhase phase = OperatorPhase.PHASE_1of2;  // default phase
  private final List<String> functions;

  @JsonCreator
  public StatisticsMerge(
      @JsonProperty("child") PhysicalOperator child,
      @JsonProperty("phase") OperatorPhase phase,
      @JsonProperty("functions") List<String> functions) {
    super(child);
    this.phase = phase;
    this.functions = ImmutableList.copyOf(functions);
  }

  public OperatorPhase getPhase() {
    return phase;
  }

  public List<String> getFunctions() {
    return functions;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value)
      throws E {
    return physicalVisitor.visitStatisticsMerge(this, value);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new StatisticsMerge(child, phase, functions);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.STATISTICS_MERGE_VALUE;
  }
}
