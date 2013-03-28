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

import java.util.Collection;
import java.util.List;

import org.apache.drill.common.expression.visitors.OpVisitor;
import org.apache.drill.common.logical.ValidationError;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;

@JsonPropertyOrder({"@id", "memo", "input"}) // op will always be first since it is wrapped.
@JsonIdentityInfo(generator=ObjectIdGenerators.IntSequenceGenerator.class, property="@id")
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property="op")
public interface LogicalOperator extends Iterable<LogicalOperator>{
	
	//public static final Class<?>[] SUB_TYPES = {Write.class, CollapsingAggregate.class, Segment.class, Filter.class, Flatten.class, Join.class, Order.class, Limit.class, Project.class, Scan.class, Sequence.class, Transform.class, Union.class, WindowFrame.class};
	
	public void accept(OpVisitor visitor);
  public void registerAsSubscriber(LogicalOperator operator);
  public void unregisterSubscriber(LogicalOperator operator);
	public void setupAndValidate(List<LogicalOperator> operators, Collection<ValidationError> errors);
	
	

}
