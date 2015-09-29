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
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;

import com.google.common.collect.ImmutableList;

import org.apache.drill.exec.vector.complex.RepeatedValueVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FlattenTemplate implements Flattener {
  private static final Logger logger = LoggerFactory.getLogger(FlattenTemplate.class);

  private static final int OUTPUT_BATCH_SIZE = 4*1024;
  private static final int OUTPUT_MEMORY_LIMIT = 512 * 1024 * 1024;

  private ImmutableList<TransferPair> transfers;
  private BufferAllocator outputAllocator;
  private SelectionVectorMode svMode;
  private RepeatedValueVector fieldToFlatten;
  private RepeatedValueVector.RepeatedAccessor accessor;
  private int valueIndex;
  private boolean bigRecords = false;
  private int bigRecordsBufferSize;

  /**
   * The output batch limit starts at OUTPUT_BATCH_SIZE, but may be decreased
   * if records are found to be large.
   */
  private int outputLimit = OUTPUT_BATCH_SIZE;

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
  public final int flattenRecords(final int recordCount, final int firstOutputIndex,
      final Flattener.Monitor monitor) {
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
          int recordsThisCall = 0;
          final int valueCount = accessor.getValueCount();
          for ( ; valueIndexLocal < valueCount; valueIndexLocal++) {
            final int innerValueCount = accessor.getInnerValueCountAt(valueIndexLocal);
            for ( ; innerValueIndexLocal < innerValueCount; innerValueIndexLocal++) {
              // If we've hit the batch size limit, stop and flush what we've got so far.
              if (recordsThisCall == outputLimit) {
                if (bigRecords) {
                  /*
                   * We got to the limit we used before, but did we go over
                   * the bigRecordsBufferSize in the second half of the batch? If
                   * so, we'll need to adjust the batch limits.
                   */
                  adjustBatchLimits(1, monitor, recordsThisCall);
                }

                // Flush this batch.
                break outer;
              }

              /*
               * At the moment, the output record includes the input record, so for very
               * large records that we're flattening, we're carrying forward the original
               * record as well as the flattened element. We've seen a case where flattening a 4MB
               * record with a 20,000 element array causing memory usage to explode. To avoid
               * that until we can push down the selected fields to operators like this, we
               * also limit the amount of memory in use at one time.
               *
               * We have to have written at least one record to be able to get a buffer that will
               * have a real allocator, so we have to do this lazily. We won't check the limit
               * for the first two records, but that keeps this simple.
               */
              if (bigRecords) {
                /*
                 * If we're halfway through the outputLimit, check on our memory
                 * usage so far.
                 */
                if (recordsThisCall == outputLimit / 2) {
                  /*
                   * If we've used more than half the space we've used for big records
                   * in the past, we've seen even bigger records than before, so stop and
                   * see if we need to flush here before we go over bigRecordsBufferSize
                   * memory usage, and reduce the outputLimit further before we continue
                   * with the next batch.
                   */
                  if (adjustBatchLimits(2, monitor, recordsThisCall)) {
                    break outer;
                  }
                }
              } else {
                if (outputAllocator.getAllocatedMemory() > OUTPUT_MEMORY_LIMIT) {
                  /*
                   * We're dealing with big records. Reduce the outputLimit to
                   * the current record count, and take note of how much space the
                   * vectors report using for that. We'll use those numbers as limits
                   * going forward in order to avoid allocating more memory.
                   */
                  bigRecords = true;
                  outputLimit = Math.min(recordsThisCall, outputLimit);
                  if (outputLimit < 1) {
                    throw new IllegalStateException("flatten outputLimit (" + outputLimit
                        + ") won't make progress");
                  }

                  /*
                   * This will differ from what the allocator reports because of
                   * overhead. But the allocator check is much cheaper to do, so we
                   * only compute this at selected times.
                   */
                  bigRecordsBufferSize = monitor.getBufferSizeFor(recordsThisCall);

                  // Stop and flush.
                  break outer;
                }
              }

              try {
                doEval(valueIndexLocal, outputIndex);
              } catch (OversizedAllocationException ex) {
                // unable to flatten due to a soft buffer overflow. split the batch here and resume execution.
                logger.debug("Reached allocation limit. Splitting the batch at input index: {} - inner index: {} - current completed index: {}",
                    valueIndexLocal, innerValueIndexLocal, currentInnerValueIndexLocal) ;

                /*
                 * TODO
                 * We can't further reduce the output limits here because it won't have
                 * any effect. The vectors have already gotten large, and there's currently
                 * no way to reduce their size. Ideally, we could reduce the outputLimit,
                 * and reduce the size of the currently used vectors.
                 */
                break outer;
              }
              outputIndex++;
              currentInnerValueIndexLocal++;
              ++recordsThisCall;
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

  /**
   * Determine if the current batch record limit needs to be adjusted (when handling
   * bigRecord mode). If so, adjust the limit, and return true, otherwise return false.
   *
   * <p>If the limit is adjusted, it will always be adjusted down, because we need to operate
   * based on the largest sized record we've ever seen.</p>
   *
   * <p>If the limit is adjusted, then the current batch should be flushed, because
   * continuing would lead to going over the large memory limit that has already been
   * established.</p>
   *
   * @param multiplier Multiply currently used memory (according to the monitor) before
   *   checking against past memory limits. This allows for checking the currently used
   *   memory after processing a fraction of the expected batch limit, but using that as
   *   a predictor of the full batch's size. For example, if this is checked after half
   *   the batch size limit's records are processed, then using a multiplier of two will
   *   do the check under the assumption that processing the full batch limit will use
   *   twice as much memory.
   * @param monitor the Flattener.Monitor instance to use for the current memory usage check
   * @param recordsThisCall the number of records processed so far during this call to
   *   flattenRecords().
   * @return true if the batch size limit was adjusted, false otherwise
   */
  private boolean adjustBatchLimits(final int multiplier, final Flattener.Monitor monitor,
      final int recordsThisCall) {
    assert bigRecords : "adjusting batch limits when no big records";
    final int bufferSize = multiplier * monitor.getBufferSizeFor(recordsThisCall);

    /*
     * If the amount of space we've used so far is below the amount that triggered
     * the bigRecords mode, then no adjustment is needed.
     */
    if (bufferSize <= bigRecordsBufferSize) {
      return false;
    }

    /*
     * We've used more space than we've used for big records in the past, we've seen
     * even bigger records, so we need to adjust our limits, and flush what we've got so far.
     *
     * We should reduce the outputLimit proportionately to get the predicted
     * amount of memory used back down to bigRecordsBufferSize.
     *
     * The number of records to limit is therefore
     * outputLimit *
     *   (1 - (bufferSize - bigRecordsBufferSize) / bigRecordsBufferSize)
     *
     * Doing some algebra on the multiplier:
     * (bigRecordsBufferSize - (bufferSize - bigRecordsBufferSize)) / bigRecordsBufferSize
     * (bigRecordsBufferSize - bufferSize + bigRecordsBufferSize) / bigRecordsBufferSize
     * (2 * bigRecordsBufferSize - bufferSize) / bigRecordsBufferSize
     *
     * If bufferSize has gotten so big that this would be negative, we'll
     * just go down to one record per batch. We need to check for that on
     * outputLimit anyway, in order to make sure that we make progress.
     */
    final int newLimit = (int)
        (outputLimit * (2.0 * ((double) bigRecordsBufferSize) - bufferSize) / bigRecordsBufferSize);
    outputLimit = Math.max(1, newLimit);
    return true;
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
    outputAllocator = outgoing.getOutgoingContainer().getOperatorContext().getAllocator();
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
