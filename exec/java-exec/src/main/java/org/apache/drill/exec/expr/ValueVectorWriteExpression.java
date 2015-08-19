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
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.record.TypedFieldId;

import com.google.common.collect.Iterators;

public class ValueVectorWriteExpression implements LogicalExpression {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ValueVectorWriteExpression.class);

  private final TypedFieldId fieldId;
  private final LogicalExpression child;
  private final boolean safe;
  /**
   * When writing into a value vector the default assumption is that the destination vector is empty, so we don't need
   * to write null values and we can just "skip" their position. Setting writeNulls to true will override this behavior
   * and explicitly write null values into the destination value vector. This is especially useful when the value vector
   * is used as an internal storage that can be reused without clearing it first.
   */
  private final boolean writeNulls;

  public ValueVectorWriteExpression(TypedFieldId fieldId, LogicalExpression child){
    this(fieldId, child, false, false);
  }

  public ValueVectorWriteExpression(TypedFieldId fieldId, LogicalExpression child, boolean safe){
    this(fieldId, child, safe, false);
  }

  public ValueVectorWriteExpression(TypedFieldId fieldId, LogicalExpression child, boolean safe, boolean writeNulls){
    this.fieldId = fieldId;
    this.child = child;
    this.safe = safe;
    this.writeNulls = writeNulls;
  }

  public TypedFieldId getFieldId() {
    return fieldId;
  }

  @Override
  public MajorType getMajorType() {
    return Types.NULL;
  }


  public boolean isSafe() {
    return safe;
  }

  /**
   * @return true if NULL values are explicitly written
   */
  public boolean isWriteNulls() {
    return writeNulls;
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitUnknown(this, value);
  }

  @Override
  public ExpressionPosition getPosition() {
    return ExpressionPosition.UNKNOWN;
  }

  public LogicalExpression getChild() {
    return child;
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return Iterators.singletonIterator(child);
  }

  @Override
  public int getSelfCost() {
    return 0;  // TODO
  }

  @Override
  public int getCumulativeCost() {
    return 0; // TODO
  }


}
