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
package org.apache.drill.exec.planner.physical;

import java.util.List;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.exec.expr.fn.impl.HashFunctions;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.reltype.RelDataType;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ImmutableList;

public class PrelUtil {

  public static List<Ordering> getOrdering(RelCollation collation, RelDataType rowType) {
    List<Ordering> orderExpr = Lists.newArrayList();
    
    final List<String> childFields = rowType.getFieldNames();
    
    for (RelFieldCollation fc: collation.getFieldCollations() ) {      
      FieldReference fr = new FieldReference(childFields.get(fc.getFieldIndex()), ExpressionPosition.UNKNOWN);
      orderExpr.add(new Ordering(fc.getDirection(), fr, fc.nullDirection));
    }
    
    return orderExpr;
  }

  /*
   * Return a hash expression :  hash(field1) ^ hash(field2) ^ hash(field3) ... ^ hash(field_n)
   */
  public static LogicalExpression getHashExpression(List<DistributionField> fields, RelDataType rowType) {
    assert fields.size() > 0;
    
    final List<String> childFields = rowType.getFieldNames();
    
    FieldReference fr = new FieldReference(childFields.get(fields.get(0).getFieldId()), ExpressionPosition.UNKNOWN);    
    FunctionCall func = new FunctionCall("hash",  ImmutableList.of((LogicalExpression)fr), ExpressionPosition.UNKNOWN);
    
    for (int i = 1; i<fields.size(); i++) {     
      fr = new FieldReference(childFields.get(fields.get(i).getFieldId()), ExpressionPosition.UNKNOWN);      
      FunctionCall func2 = new FunctionCall("hash",  ImmutableList.of((LogicalExpression)fr), ExpressionPosition.UNKNOWN);
      
      func = new FunctionCall("xor", ImmutableList.of((LogicalExpression)func, (LogicalExpression)func2), ExpressionPosition.UNKNOWN);
    }
    
    return func;
  }

}
