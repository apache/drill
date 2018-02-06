/*
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
package org.apache.drill.exec.physical.impl.svremover;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.ValueVector;

public class GenericSV4Copier extends AbstractSV4Copier {
  @Override
  public void copyEntry(int inIndex, int outIndex) throws SchemaChangeException {
    int inOffset = inIndex & 0xFFFF;
    int inVector = inIndex >>> 16;
    for ( int i = 0;  i < vvIn.length;  i++ ) {
      vvOut[i].copyEntry(outIndex, vvIn[i][inVector], inOffset);
    }
  }

  public static Copier createCopier(RecordBatch batch, VectorContainer container, SchemaChangeCallBack callBack) throws SchemaChangeException {
    for(VectorWrapper<?> vv : batch){
      @SuppressWarnings("resource")
      ValueVector v = vv.getValueVectors()[0];
      v.makeTransferPair(container.addOrGet(v.getField(), callBack));
    }

    Copier copier = new GenericSV4Copier();
    copier.setup(batch, container);
    return copier;
  }
}