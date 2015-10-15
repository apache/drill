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

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.physical.impl.project.ProjectRecordBatch;
import org.apache.drill.exec.vector.complex.impl.ComplexWriterImpl;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;

import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JVar;

public class DrillComplexWriterFuncHolder extends DrillSimpleFuncHolder{

  private FieldReference ref;

  public DrillComplexWriterFuncHolder(FunctionAttributes functionAttributes, FunctionInitializer initializer) {
    super(functionAttributes, initializer);
  }

  public void setReference(FieldReference ref) {
    this.ref = ref;
  }

  @Override
  protected HoldingContainer generateEvalBody(ClassGenerator<?> g, HoldingContainer[] inputVariables, String body, JVar[] workspaceJVars) {

    g.getEvalBlock().directStatement(String.format("//---- start of eval portion of %s function. ----//", registeredNames[0]));

    JBlock sub = new JBlock(true, true);
    JBlock topSub = sub;

    JVar complexWriter = g.declareClassField("complexWriter", g.getModel()._ref(ComplexWriter.class));

    JClass cwClass = g.getModel().ref(ComplexWriterImpl.class);

    JInvocation container = g.getMappingSet().getOutgoing().invoke("getOutgoingContainer");

    //Default name is "col", if not passed in a reference name for the output vector.
    String refName = ref == null? "col" : ref.getRootSegment().getPath();

    g.getSetupBlock().assign(complexWriter, cwClass.staticInvoke("getWriter").arg(refName).arg(container));

    JClass projBatchClass = g.getModel().ref(ProjectRecordBatch.class);
    JExpression projBatch = JExpr.cast(projBatchClass, g.getMappingSet().getOutgoing());

    g.getSetupBlock().add(projBatch.invoke("addComplexWriter").arg(complexWriter));


    g.getEvalBlock().add(complexWriter.invoke("setPosition").arg(g.getMappingSet().getValueWriteIndex()));

    sub.decl(g.getModel()._ref(ComplexWriter.class), returnValue.name, complexWriter);

    // add the subblock after the out declaration.
    g.getEvalBlock().add(topSub);

    addProtectedBlock(g, sub, body, inputVariables, workspaceJVars, false);


//    JConditional jc = g.getEvalBlock()._if(complexWriter.invoke("ok").not());

//    jc._then().add(complexWriter.invoke("reset"));
    //jc._then().directStatement("System.out.println(\"debug : write ok fail!, inIndex = \" + inIndex);");
//    jc._then()._return(JExpr.FALSE);

    //jc._else().directStatement("System.out.println(\"debug : write successful, inIndex = \" + inIndex);");

    g.getEvalBlock().directStatement(String.format("//---- end of eval portion of %s function. ----//", registeredNames[0]));

    return null;
  }

}
