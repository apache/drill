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
package org.apache.drill.exec.physical.impl.unnest;

import com.google.common.collect.ImmutableList;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.LateralContract;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.util.List;

public abstract class UnnestTemplate implements Unnest {
  private static final Logger logger = LoggerFactory.getLogger(UnnestTemplate.class);

  private static final int OUTPUT_ROW_COUNT = ValueVector.MAX_ROW_COUNT;

  private ImmutableList<TransferPair> transfers;
  private LateralContract lateral; // corresponding lateral Join (or other operator implementing the Lateral Contract)
  private SelectionVectorMode svMode;
  private RepeatedValueVector fieldToUnnest;
  private RepeatedValueVector.RepeatedAccessor accessor;

  /**
   * The output batch limit starts at OUTPUT_ROW_COUNT, but may be decreased
   * if records are found to be large.
   */
  private int outputLimit = OUTPUT_ROW_COUNT;


  // The index in the unnest column that is being processed.We start at zero and continue until
  // InnerValueCount is reached or  if the batch limit is reached
  // this allows for groups to be written between batches if we run out of space, for cases where we have finished
  // a batch on the boundary it will be set to 0
  private int innerValueIndex = 0;

  @Override
  public void setUnnestField(RepeatedValueVector unnestField) {
    this.fieldToUnnest = unnestField;
    this.accessor = RepeatedValueVector.RepeatedAccessor.class.cast(unnestField.getAccessor());
  }

  @Override
  public RepeatedValueVector getUnnestField() {
    return fieldToUnnest;
  }

  @Override
  public void setOutputCount(int outputCount) {
    outputLimit = outputCount;
  }

  @Override
  public final int unnestRecords(final int recordCount, final int firstOutputIndex) {
    switch (svMode) {
      case FOUR_BYTE:
        throw new UnsupportedOperationException("Unnest does not support selection vector inputs.");

      case TWO_BYTE:
        throw new UnsupportedOperationException("Unnest does not support selection vector inputs.");

      case NONE:
        if (innerValueIndex == -1) {
          innerValueIndex = 0;
        }

        // Current record being processed in the incoming record batch. We could keep
        // track of it ourselves, but it is better to check with the Lateral Join and get the
        // current record being processed thru the Lateral Join Contract.
        final int currentRecord = lateral.getRecordIndex();
        final int innerValueCount = accessor.getInnerValueCountAt(currentRecord);
        final int count = Math.min(Math.min(innerValueCount, outputLimit), recordCount);

        for (TransferPair t : transfers) {
          t.splitAndTransfer(innerValueIndex, count);
        }
        innerValueIndex += count;
        return count;

      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override public final void setup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing,
      List<TransferPair> transfers, LateralContract lateral) throws SchemaChangeException {

    this.svMode = incoming.getSchema().getSelectionVectorMode();
    switch (svMode) {
      case FOUR_BYTE:
        throw new UnsupportedOperationException("Unnest does not support selection vector inputs.");
      case TWO_BYTE:
        throw new UnsupportedOperationException("Unnest does not support selection vector inputs.");
    }
    this.transfers = ImmutableList.copyOf(transfers);
    this.lateral = lateral;
  }

  @Override public void resetGroupIndex() {
    this.innerValueIndex = 0;
  }

  //public abstract void doSetup(@Named("context") FragmentContext context,
  //                             @Named("incoming") RecordBatch incoming,
  //                             @Named("outgoing") RecordBatch outgoing) throws SchemaChangeException;
  //public abstract boolean doEval(@Named("inIndex") int inIndex,
  //                               @Named("outIndex") int outIndex) throws SchemaChangeException;
}
