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

import javax.inject.Named;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.AllocationHelper;


public abstract class CopierTemplate2 implements Copier{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CopierTemplate2.class);

  private SelectionVector2 sv2;
  private RecordBatch outgoing;

  @Override
  public void setupRemover(FragmentContext context, RecordBatch incoming, RecordBatch outgoing) throws SchemaChangeException{
    this.sv2 = incoming.getSelectionVector2();
    this.outgoing = outgoing;
    doSetup(context, incoming, outgoing);
  }

  @Override
  public int copyRecords(int index, int recordCount) throws SchemaChangeException {
    for(VectorWrapper<?> out : outgoing){
      MajorType type = out.getField().getType();
      if (!Types.isFixedWidthType(type) || Types.isRepeated(type)) {
        out.getValueVector().allocateNew();
      } else {
        AllocationHelper.allocate(out.getValueVector(), recordCount, 1);
      }
    }

    int outgoingPosition = 0;

    for(int svIndex = index; svIndex < index + recordCount; svIndex++, outgoingPosition++){
      doEval(sv2.getIndex(svIndex), outgoingPosition);
    }
    return outgoingPosition;
  }

  public abstract void doSetup(@Named("context") FragmentContext context,
                               @Named("incoming") RecordBatch incoming,
                               @Named("outgoing") RecordBatch outgoing)
                       throws SchemaChangeException;
  public abstract void doEval(@Named("inIndex") int inIndex,
                              @Named("outIndex") int outIndex)
                       throws SchemaChangeException;
}
