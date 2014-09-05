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
package org.apache.drill.exec.expr.fn.interpreter;

import com.google.common.collect.Maps;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JFieldVar;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.apache.drill.exec.expr.fn.DrillSimpleFuncHolder;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.record.RecordBatch;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class InterpreterGenerator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InterpreterGenerator.class);

  public static final String PACKAGE_NAME = "org.apache.drill.exec.fn.interpreter.generated";
  private static final String ARG_NAME = "args";
  private static final String SETUP_METHOD = "doSetup";
  private static final String EVAL_METHOD = "doEval";
  public static final String INTERPRETER_CLASSNAME_POSTFIX = "Interpreter";

  private JCodeModel model ;
  private DrillSimpleFuncHolder holder;
  private JDefinedClass clazz;
  private String className;
  private String genSourceDir;

  public InterpreterGenerator (DrillSimpleFuncHolder holder, String className, String genSourceDir) {
    this.model = new JCodeModel();
    this.holder = holder;
    this.className = className;
    this.genSourceDir = genSourceDir;
  }

  public void build() throws Exception {
    try {
      this.clazz = model._class(PACKAGE_NAME + "." + className);
      JClass iface = model.ref(DrillSimpleFuncInterpreter.class);
      clazz._implements(iface);

      Map<DrillFuncHolder.WorkspaceReference, JFieldVar> wsFieldVars = Maps.newHashMap();
      for (DrillFuncHolder.WorkspaceReference ws : holder.getWorkspaceVars()) {
        JFieldVar wsVar = clazz.field(JMod.PRIVATE, ws.getType(), ws.getName());
        wsFieldVars.put(ws, wsVar);
      }

      generateSetup(wsFieldVars);

      generateEval();

      // generate the java source code for the function.
      File file = new File(genSourceDir);
      if (!file.exists()) {
        file.mkdir();
      }

      model.build(file);

    } catch (Exception e) {
      logger.error("Error when generate code for " + className + " error " + e);
      throw new IllegalStateException(e);
    }
  }

  private void generateSetup(Map<DrillFuncHolder.WorkspaceReference, JFieldVar> wsFieldVars) {
    JClass valueholderClass = model.ref(ValueHolder.class);

    JMethod doSetupMethod = clazz.method(JMod.PUBLIC, Void.TYPE, SETUP_METHOD);
    doSetupMethod.param(valueholderClass.array(), ARG_NAME);
    JVar incomingJVar = doSetupMethod.param(model.ref(RecordBatch.class), "incoming");

    if (holder.getSetupBody()!=null && ! holder.getSetupBody().trim().equals("{}")) {
      declareAssignParm(model, doSetupMethod.body(), holder, ARG_NAME, true);
    }

    for (DrillFuncHolder.WorkspaceReference ws : holder.getWorkspaceVars()) {
      if (ws.isInject()) {
        doSetupMethod.body().assign(wsFieldVars.get(ws), incomingJVar.invoke("getContext").invoke("getManagedBuffer"));
      }
    }

    doSetupMethod.body().directStatement(holder.getSetupBody());
  }

  private void generateEval() {
    JClass valueholderClass = model.ref(ValueHolder.class);

    JMethod doEvalMethod = clazz.method(JMod.PUBLIC, ValueHolder.class, EVAL_METHOD);
    doEvalMethod.param(valueholderClass.array(), ARG_NAME);

    if (holder.getEvalBody()!=null && ! holder.getEvalBody().trim().equals("")) {
      declareAssignParm(model, doEvalMethod.body(), holder, ARG_NAME, false);
    }

    DrillFuncHolder.ValueReference returnValue = holder.getReturnValue();
    JType outType = TypeHelper.getHolderType(model, returnValue.getType().getMinorType(), returnValue.getType().getMode());

    JVar outVar = doEvalMethod.body().decl(outType, returnValue.getName(), JExpr._new(outType));

    doEvalMethod.body().directStatement(holder.getEvalBody());
    doEvalMethod.body()._return(outVar);
  }

  private void declareAssignParm(JCodeModel model, JBlock block, DrillFuncHolder holder, String argName, boolean constantOnly) {

    int index = 0;
    DrillFuncHolder.ValueReference[] parameters = holder.getParameters();

    for (DrillFuncHolder.ValueReference parm : parameters) {
      JType type = TypeHelper.getHolderType(model, parm.getType().getMinorType(), parm.getType().getMode());
      block.decl(type, parm.getName(), JExpr.cast(type, JExpr.component(JExpr.ref(argName), JExpr.lit(index++))));
    }
  }

}
