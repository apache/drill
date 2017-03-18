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

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Generic selection vector 4 copier implementation that can
 * be used in place of the generated version. Relies on a
 * virtual function in each value vector to choose the proper
 * implementation. Tests suggest that this version performs
 * better than the generated version for queries with many columns.
 */

public class GenericSV4Copier extends CopierTemplate4 {

  private ValueVector[] vvOut;
  private ValueVector[][] vvIn;

  @SuppressWarnings("unused")
  @Override
  public void doSetup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing) {

    // Seems to be no way to get the vector count without iterating...

    int count = 0;
    for(VectorWrapper<?> vv : incoming) {
      count++;
    }
    vvIn = new ValueVector[count][];
    vvOut = new ValueVector[count];
    int i = 0;
    for(VectorWrapper<?> vv : incoming) {
      vvIn[i] = incoming.getValueAccessorById(ValueVector.class, i).getValueVectors();
      vvOut[i] = outgoing.getValueAccessorById(ValueVector.class, i).getValueVector();
      i++;
    }
  }

  @Override
  public void doEval(int inIndex, int outIndex) {
    int inOffset = inIndex & 0xFFFF;
    int inVector = inIndex >>> 16;
    for ( int i = 0;  i < vvIn.length;  i++ ) {
      vvOut[i].copyEntry(outIndex, vvIn[i][inVector], inOffset);
    }
  }
}
