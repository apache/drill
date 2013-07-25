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
package org.apache.drill.exec.physical.impl;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;

public abstract class FilterRecordBatch implements RecordBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FilterRecordBatch.class);

  private RecordBatch incoming;
  private SelectionVector2 selectionVector;
  private BatchSchema schema;
  private FilteringRecordBatchTransformer transformer;
  private int outstanding;

  public FilterRecordBatch(RecordBatch batch) {
    this.incoming = batch;
  }

  @Override
  public FragmentContext getContext() {
    return incoming.getContext();
  }

  @Override
  public BatchSchema getSchema() {
    return schema;
  }

  @Override
  public int getRecordCount() {
    return 0;
  }

  @Override
  public void kill() {
    incoming.kill();
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
  public <T extends ValueVector> T getValueVectorById(int fieldId, Class<?> vvClass) {
    return null;
  }

  public WritableBatch getWritableBatch() {
    return null;
  }

  abstract int applyFilter(SelectionVector2 vector, int count);

  /**
   * Release all assets.
   */
  private void close() {

  }

  @Override
  public IterOutcome next() {
    while (true) {
      IterOutcome o = incoming.next();
      switch (o) {
      case OK_NEW_SCHEMA:
        transformer = null;
        schema = transformer.getSchema();
        // fall through to ok.
      case OK:

      case NONE:
      case STOP:
        close();
        return IterOutcome.STOP;
      }

      if (outstanding > 0) {
        // move data to output location.

        for (int i = incoming.getRecordCount() - outstanding; i < incoming.getRecordCount(); i++) {

        }
      }

//      // make sure the bit vector is as large as the current record batch.
//      if (selectionVector.capacity() < incoming.getRecordCount()) {
//        selectionVector.allocateNew(incoming.getRecordCount());
//      }

      return null;
    }

  }
}
