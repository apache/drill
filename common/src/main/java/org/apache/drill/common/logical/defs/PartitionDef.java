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
package org.apache.drill.common.logical.defs;

import org.apache.drill.common.expression.LogicalExpression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PartitionDef {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PartitionDef.class);

  private final PartitionType partitionType;
  private final LogicalExpression[] expressions;
  private final LogicalExpression[] starts;
  
  @JsonCreator
  public PartitionDef(@JsonProperty("partitionType") PartitionType partitionType, @JsonProperty("exprs") LogicalExpression[] expressions, @JsonProperty("starts") LogicalExpression[] starts) {
    this.partitionType = partitionType;
    this.expressions = expressions;
    this.starts = starts;
  }

  public PartitionType getPartitionType() {
    return partitionType;
  }

  public LogicalExpression[] getExpressions() {
    return expressions;
  }

  public LogicalExpression[] getStarts() {
    return starts;
  }
  

  public static enum PartitionType{ 
    RANDOM, HASH, ORDERED;
  };
}
