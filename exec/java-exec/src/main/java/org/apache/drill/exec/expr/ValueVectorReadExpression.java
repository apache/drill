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
package org.apache.drill.exec.expr;

import java.util.Iterator;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.record.TypedFieldId;

import com.google.common.collect.Iterators;

import javax.sound.sampled.FloatControl;

public class ValueVectorReadExpression implements LogicalExpression{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ValueVectorReadExpression.class);

  private MajorType type;
  private final TypedFieldId fieldId;
  private final boolean superReader;
  private final int index;
  private final boolean isArrayElement;
  
  
  public ValueVectorReadExpression(TypedFieldId tfId, int index, boolean isArrayElement){
    this.type = tfId.getType();
    this.fieldId = tfId;
    this.superReader = tfId.isHyperReader();
    this.index = index;
    this.isArrayElement = isArrayElement;
  }

  public void required() {
    type = Types.required(type.getMinorType());
  }

  public boolean isArrayElement() {
    return isArrayElement;
  }

  public ValueVectorReadExpression(TypedFieldId tfId) {
    this(tfId, -1, false);
  }
  
  public TypedFieldId getTypedFieldId(){
    return fieldId;
  }
  
  public boolean isSuperReader(){
    return superReader;
  }
  @Override
  public MajorType getMajorType() {
    return type;
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitUnknown(this, value);
  }

  public TypedFieldId getFieldId() {
    return fieldId;
  }

  public int getIndex() {
    return index;
  }

  @Override
  public ExpressionPosition getPosition() {
    return ExpressionPosition.UNKNOWN;
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return Iterators.emptyIterator();
  }
  
  
}
