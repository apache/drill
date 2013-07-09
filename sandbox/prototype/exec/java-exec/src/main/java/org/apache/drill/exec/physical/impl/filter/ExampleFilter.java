/*******************************************************************************
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
 ******************************************************************************/

package org.apache.drill.exec.physical.impl.filter;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.InvalidValueAccessor;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.TypedFieldId;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.vector.SelectionVector2;
import org.apache.drill.exec.record.vector.SelectionVector4;
import org.apache.drill.exec.record.vector.ValueVector;

public class ExampleFilter implements RecordBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExampleFilter.class);

  //private EvalutationPredicates []
  private RecordBatch incoming;
  private BatchSchema outboundSchema;
  private int recordCount;

  private void reconfigureSchema() throws SchemaChangeException {
    BatchSchema in = incoming.getSchema();
    outboundSchema = BatchSchema.newBuilder().addFields(in).setSelectionVectorMode(BatchSchema.SelectionVectorMode.TWO_BYTE).build();
  }

  private int generateSelectionVector(){
                    return -1;
  }

  @Override
  public FragmentContext getContext() {
    return incoming.getContext();
  }

  @Override
  public BatchSchema getSchema() {
    return outboundSchema;
  }

  @Override
  public int getRecordCount() {
    return recordCount;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void kill() {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    return null;
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    return null;
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    return null;
  }

  @Override
  public <T extends ValueVector<T>> T getValueVectorById(int fieldId, Class<?> vvClass) {
    return null;
  }

  @Override
  public IterOutcome next() {
    IterOutcome out = incoming.next();
    switch (incoming.next()) {

      case NONE:
        return IterOutcome.NONE;
      case OK_NEW_SCHEMA:
        //reconfigureSchema();
      case OK:
        this.recordCount = generateSelectionVector();
        return out;
      case STOP:
        return IterOutcome.STOP;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public WritableBatch getWritableBatch() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
