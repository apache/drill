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
package org.apache.drill.exec.physical.base;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.graph.GraphValue;
import org.apache.drill.exec.physical.OperatorCost;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;

@JsonInclude(Include.NON_NULL)
@JsonPropertyOrder({ "@id" })
@JsonIdentityInfo(generator = ObjectIdGenerators.IntSequenceGenerator.class, property = "@id")
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "pop")
public interface  PhysicalOperator extends GraphValue<PhysicalOperator> {

  /**
   * Get the cost of execution of this particular operator.
   * 
   * @return
   */
  @JsonIgnore
  public OperatorCost getCost();
  
  /**
   * Get the estimated size of this particular operator.
   * @return
   */
  @JsonIgnore
  public Size getSize();
  
  /**
   * Describes whether or not a particular physical operator can actually be executed. Most physical operators can be
   * executed. However, Exchange nodes cannot be executed. In order to be executed, they must be converted into their
   * Exec sub components.
   * 
   * @return
   */
  @JsonIgnore
  public boolean isExecutable();

  /**
   * Provides capability to build a set of output based on traversing a query graph tree.
   * 
   * @param physicalVisitor
   * @return
   */
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E;

  /**
   * Regenerate with this node with a new set of children.  This is used in the case of materialization or optimization.
   * @param children
   * @return
   */
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException;

}
