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
package org.apache.drill.exec.expr.fn;

import com.google.common.collect.Lists;

import com.sun.codemodel.JBlock;
import com.sun.codemodel.JCatchBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JFieldVar;
import com.sun.codemodel.JPrimitiveType;
import com.sun.codemodel.JTryBlock;
import com.sun.codemodel.JVar;

import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CloneVisitor;
import org.apache.drill.exec.expr.DrillFuncHolderExpr;

import java.util.List;
import java.util.Map;

public class DrillSimpleFuncHolderError extends DrillSimpleFuncHolder {
  public static final String DRILL_ERROR_CODE_ARRAY = "errorCodeList";
  public static final String DRILL_ERROR_CODE_INDEX = "errorCodeIndex";

  private DrillSimpleFuncHolder innerFuncHolder;
  public DrillSimpleFuncHolderError(DrillSimpleFuncHolder innerFuncHolder) {
    super(innerFuncHolder.getFunctionAttributes(), innerFuncHolder.getInitializer());
    this.innerFuncHolder = innerFuncHolder;
  }

  @Override
  public boolean isNested() {
    return innerFuncHolder.isNested();
  }

  @Override
  public void exceptionHandling(ClassGenerator<?> g, JBlock topSub) {
    final JTryBlock tryBlock = g.getEvalBlock()._try();
    final JBlock tryBody = tryBlock.body();
    tryBody.add(topSub);

    final JCatchBlock jCatchBlock = tryBlock._catch(g.getModel().directClass(Exception.class.getCanonicalName()));
    final JBlock catchBody = jCatchBlock.body();

    JFieldVar point = null;
    for(Map.Entry<String, JFieldVar> entry : g.clazz.fields().entrySet()) {
      if(entry.getKey().contains(DRILL_ERROR_CODE_ARRAY)) {
        point = entry.getValue();
        break;
      }
    }
    assert point != null;

    JVar indexInErrorCodeArray = null;
    for(Map.Entry<String, JFieldVar> entry : g.clazz.fields().entrySet()) {
      if(entry.getKey().contains(DRILL_ERROR_CODE_INDEX)) {
        indexInErrorCodeArray = entry.getValue();
        break;
      }
    }

    assert indexInErrorCodeArray != null;

    final JClass intArrayType = JPrimitiveType.parse(g.getModel(), "int").array();
    final JVar intArray = catchBody.decl(intArrayType, "intArray");
    catchBody.assign(intArray, JExpr.cast(intArrayType, point.invoke("get").arg(indexInErrorCodeArray)));
    catchBody.assign(intArray.component(JExpr.lit(0)), JExpr.lit(1));
  }

  public static LogicalExpression wrapError(LogicalExpression expr) {
    return expr.accept(new CloneVisitor() {
      @Override
      public LogicalExpression visitFunctionHolderExpression(FunctionHolderExpression holder, Void value) throws RuntimeException {
        if (holder instanceof DrillFuncHolderExpr && holder.getHolder() instanceof DrillSimpleFuncHolder) {
          List<LogicalExpression> args = Lists.newArrayList();
          for (LogicalExpression arg : holder.args) {
            args.add(arg.accept(this, null));
          }
          return new DrillFuncHolderExpr(holder.getName(),
              new DrillSimpleFuncHolderError((DrillSimpleFuncHolder) holder.getHolder()),
              args,
              holder.getPosition());
        }
        throw new RuntimeException();
      }
    }, null);
  }
}