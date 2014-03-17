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
package org.apache.drill.exec.compile.sig;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.visitors.ExprVisitor;

import com.beust.jcommander.internal.Lists;

public class ConstantExpressionIdentifier implements ExprVisitor<Boolean, IdentityHashMap<LogicalExpression, Object>, RuntimeException>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ConstantExpressionIdentifier.class);

  private ConstantExpressionIdentifier(){}
  
  /**
   * Get a list of expressions that mark boundaries into a constant space.
   * @param e
   * @return
   */
  public static Set<LogicalExpression> getConstantExpressionSet(LogicalExpression e){
    IdentityHashMap<LogicalExpression, Object> map = new IdentityHashMap<>();
    ConstantExpressionIdentifier visitor = new ConstantExpressionIdentifier();
    

    if(e.accept(visitor, map) && map.isEmpty()){
      // if we receive a constant value here but the map is empty, this means the entire tree is a constant.
      // note, we can't use a singleton collection here because we need an identity set.
      map.put(e, true);
      return map.keySet();
    }else if(map.isEmpty()){
      // so we don't continue to carry around a map, we let it go here and simply return an empty set.
      return Collections.emptySet();
    }else{
      return map.keySet();  
    }
  }

  private boolean checkChildren(LogicalExpression e, IdentityHashMap<LogicalExpression, Object> value, boolean transmitsConstant){
    List<LogicalExpression> constants = Lists.newLinkedList();
    boolean constant = true;

    for(LogicalExpression child : e){
      if(child.accept(this, value)){
        constants.add(child);
      }else{
        constant = false;
      }
    }
    
    // if one or more clauses isn't constant, this isn't constant.  this also isn't a constant if it operates on a set.
    if(!constant || !transmitsConstant){
      for(LogicalExpression c: constants){
        value.put(c, true);
      }
    }
    return constant && transmitsConstant;
  }
  
  @Override
  public Boolean visitFunctionCall(FunctionCall call, IdentityHashMap<LogicalExpression, Object> value){
    return checkChildren(call, value, !call.getDefinition().isAggregating() && !call.getDefinition().isRandom());
  }

  
  @Override
  public Boolean visitIfExpression(IfExpression ifExpr, IdentityHashMap<LogicalExpression, Object> value){
    return checkChildren(ifExpr, value, true);
  }

  @Override
  public Boolean visitSchemaPath(SchemaPath path, IdentityHashMap<LogicalExpression, Object> value){
    return false;
  }

  @Override
  public Boolean visitIntConstant(ValueExpressions.IntExpression intExpr, IdentityHashMap<LogicalExpression, Object> value) throws RuntimeException {
    value.put(intExpr, true);
    return true;
  }

  @Override
  public Boolean visitFloatConstant(ValueExpressions.FloatExpression fExpr, IdentityHashMap<LogicalExpression, Object> value) throws RuntimeException {
    value.put(fExpr, true);
    return true;
  }

  @Override
  public Boolean visitLongConstant(LongExpression intExpr, IdentityHashMap<LogicalExpression, Object> value){
    return true;
  }

  @Override
  public Boolean visitDoubleConstant(DoubleExpression dExpr, IdentityHashMap<LogicalExpression, Object> value){
    return true;
  }

  @Override
  public Boolean visitBooleanConstant(BooleanExpression e, IdentityHashMap<LogicalExpression, Object> value){
    return true;
  }

  @Override
  public Boolean visitQuotedStringConstant(QuotedString e, IdentityHashMap<LogicalExpression, Object> value){
    return true;
  }

  @Override
  public Boolean visitUnknown(LogicalExpression e, IdentityHashMap<LogicalExpression, Object> value){
    return checkChildren(e, value, false);
  }
  
}
