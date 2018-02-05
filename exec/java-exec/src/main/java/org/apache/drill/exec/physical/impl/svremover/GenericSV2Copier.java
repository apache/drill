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
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;

public class GenericSV2Copier extends CopierTemplate2 {

  private ValueVector[] vvOut;
  private ValueVector[] vvIn;

  @SuppressWarnings("unused")
  @Override
  public void doSetup(FragmentContext context, RecordBatch incoming,
                      RecordBatch outgoing) throws SchemaChangeException {

    int count = 0;
    for(VectorWrapper<?> vv : incoming) {
      count++;
    }
    vvIn = new ValueVector[count];
    vvOut = new ValueVector[count];
    int i = 0;
    for(VectorWrapper<?> vv : incoming) {
      vvIn[i] = incoming.getValueAccessorById(ValueVector.class, i).getValueVector();
      vvOut[i] = outgoing.getValueAccessorById(ValueVector.class, i).getValueVector();
      i++;
    }
  }

  @Override
  public void doEval(int inIndex, int outIndex) throws SchemaChangeException {
    for ( int i = 0;  i < vvIn.length;  i++ ) {
      vvOut[i].copyEntry(outIndex, vvIn[i], inIndex);
    }
  }
}