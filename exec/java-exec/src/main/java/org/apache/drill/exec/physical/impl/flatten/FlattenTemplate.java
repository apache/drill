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
package org.apache.drill.exec.physical.impl.flatten;

import java.util.List;

import javax.inject.Named;

import org.apache.drill.exec.exception.OversizedAllocationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

import com.google.common.collect.ImmutableList;

import org.apache.drill.exec.vector.complex.RepeatedValueVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FlattenTemplate implements Flattener {
  private static final Logger logger = LoggerFactory.getLogger(FlattenTemplate.class);

  private static final int OUTPUT_BATCH_SIZE = 4*1024;

  private ImmutableList<TransferPair> transfers;
  private SelectionVector2 vector2;
  private SelectionVector4 vector4;
  private SelectionVectorMode svMode;
  private RepeatedValueVector fieldToFlatten;
  private RepeatedValueVector.RepeatedAccessor accessor;
  private int valueIndex;

  // this allows for groups to be written between batches if we run out of space, for cases where we have finished
  // a batch on the boundary it will be set to 0
  private int innerValueIndex;
  private int currentInnerValueIndex;

  public FlattenTemplate() throws SchemaChangeException {
    innerValueIndex = -1;
  }

  @Override
  public void setFlattenField(RepeatedValueVector flattenField) {
    this.fieldToFlatten = flattenField;
    this.accessor = RepeatedValueVector.RepeatedAccessor.class.cast(flattenField.getAccessor());
  }

  public RepeatedValueVector getFlattenField() {
    return fieldToFlatten;
  }

  @Override
  public final int flattenRecords(final int recordCount, final int firstOutputIndex) {
    switch (svMode) {
      case FOUR_BYTE:
        throw new UnsupportedOperationException("Flatten does not support selection vector inputs.");

      case TWO_BYTE:
        throw new UnsupportedOperationException("Flatten does not support selection vector inputs.");

      case NONE:
        if (innerValueIndex == -1) {
          innerValueIndex = 0;
        }
        final int initialInnerValueIndex = currentInnerValueIndex;
        // restore state to local stack
        int valueIndexLocal = valueIndex;
        int innerValueIndexLocal = innerValueIndex;
        int currentInnerValueIndexLocal = currentInnerValueIndex;
        outer: {
          int outputIndex = firstOutputIndex;
          final int valueCount = accessor.getValueCount();
          for ( ; valueIndexLocal < valueCount; valueIndexLocal++) {
            final int innerValueCount = accessor.getInnerValueCountAt(valueIndexLocal);
            for ( ; innerValueIndexLocal < innerValueCount; innerValueIndexLocal++) {
              if (outputIndex == OUTPUT_BATCH_SIZE) {
                break outer;
              }
              try {
                doEval(valueIndexLocal, outputIndex);
              } catch (OversizedAllocationException ex) {
                // unable to flatten due to a soft buffer overflow. split the batch here and resume execution.
                logger.debug("Reached allocation limit. Splitting the batch at input index: {} - inner index: {} - current completed index: {}",
                    valueIndexLocal, innerValueIndexLocal, currentInnerValueIndexLocal) ;
                break outer;
              }
              outputIndex++;
              currentInnerValueIndexLocal++;
            }
            innerValueIndexLocal = 0;
          }
        }
        // save state to heap
        valueIndex = valueIndexLocal;
        innerValueIndex = innerValueIndexLocal;
        currentInnerValueIndex = currentInnerValueIndexLocal;
        // transfer the computed range
        final int delta = currentInnerValueIndexLocal - initialInnerValueIndex;
        for (TransferPair t : transfers) {
          t.splitAndTransfer(initialInnerValueIndex, delta);
        }
        return delta;

      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public final void setup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing, List<TransferPair> transfers)  throws SchemaChangeException{

    this.svMode = incoming.getSchema().getSelectionVectorMode();
    switch (svMode) {
      case FOUR_BYTE:
        throw new UnsupportedOperationException("Flatten does not support selection vector inputs.");
      case TWO_BYTE:
        throw new UnsupportedOperationException("Flatten does not support selection vector inputs.");
    }
    this.transfers = ImmutableList.copyOf(transfers);
    doSetup(context, incoming, outgoing);
  }



  @Override
  public void resetGroupIndex() {
    this.valueIndex = 0;
    this.currentInnerValueIndex = 0;
  }

  public abstract void doSetup(@Named("context") FragmentContext context, @Named("incoming") RecordBatch incoming, @Named("outgoing") RecordBatch outgoing);
  public abstract boolean doEval(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);

}
