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
package org.apache.drill.exec.vector;

import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JVar;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorWrapper;

public class CopyUtil {
  public static void generateCopies(ClassGenerator g, VectorAccessible batch, boolean hyper){
    // we have parallel ids for each value vector so we don't actually have to deal with managing the ids at all.
    int fieldId = 0;

    JExpression inIndex = JExpr.direct("inIndex");
    JExpression outIndex = JExpr.direct("outIndex");
    g.rotateBlock();
    for(VectorWrapper<?> vv : batch){
      JVar inVV = g.declareVectorValueSetupAndMember("incoming", new TypedFieldId(vv.getField().getType(), vv.isHyper(), fieldId));
      JVar outVV = g.declareVectorValueSetupAndMember("outgoing", new TypedFieldId(vv.getField().getType(), false, fieldId));

      if(hyper){

        g.getEvalBlock()._if(
                outVV
                        .invoke("copyFromSafe")
                        .arg(
                                inIndex.band(JExpr.lit((int) Character.MAX_VALUE)))
                        .arg(outIndex)
                        .arg(
                                inVV.component(inIndex.shrz(JExpr.lit(16)))
                        )
                        .not()
        )
                ._then()._return(JExpr.FALSE);
      }else{
        g.getEvalBlock()._if(outVV.invoke("copyFromSafe").arg(inIndex).arg(outIndex).arg(inVV).not())._then()._return(JExpr.FALSE);
      }


      fieldId++;
    }
    g.rotateBlock();
    g.getEvalBlock()._return(JExpr.TRUE);
  }

}
