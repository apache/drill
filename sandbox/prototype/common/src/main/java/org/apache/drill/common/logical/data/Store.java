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
package org.apache.drill.common.logical.data;

import org.apache.drill.common.exceptions.ExpressionParsingException;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.JSONOptions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("store")
public class Store extends SinkOperator{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Store.class);
  
  private final String storageEngine;
  private final JSONOptions target;
  private final PartitionOptions partition;

  @JsonCreator
  public Store(@JsonProperty("storageengine") String storageEngine, @JsonProperty("target") JSONOptions target, @JsonProperty("partition") PartitionOptions partition) {
    super();
    this.storageEngine = storageEngine;
    this.target = target;
    this.partition = partition;
  }

  public String getStorageEngine() {
    return storageEngine;
  }

  public JSONOptions getTarget() {
    return target;
  }

  public PartitionOptions getPartition() {
    return partition;
  }

  public static enum PartitionType{ 
    RANDOM, HASH, ORDERED;
    
    public static PartitionType resolve(String val){
      for(PartitionType pt : PartitionType.values()){
        if(pt.name().equalsIgnoreCase(val)) return pt;
      }
      throw new ExpressionParsingException(String.format("Unable to determine partitioning type type for value '%s'.", val));

    }
    
  };
  
  public static class PartitionOptions{
    private final PartitionType partitionType;
    private final LogicalExpression[] expressions;
    private final LogicalExpression[] starts;
    
    @JsonCreator
    public PartitionOptions(@JsonProperty("partitionType") String partitionType, @JsonProperty("exprs") LogicalExpression[] expressions, @JsonProperty("starts") LogicalExpression[] starts) {
      this.partitionType = PartitionType.resolve(partitionType);
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
    
    
  }
  
}
