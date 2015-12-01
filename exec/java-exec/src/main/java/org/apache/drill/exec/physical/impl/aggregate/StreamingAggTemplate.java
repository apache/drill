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
package org.apache.drill.exec.physical.impl.aggregate;

import javax.inject.Named;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.VectorWrapper;

public abstract class StreamingAggTemplate implements StreamingAggregator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StreamingAggregator.class);
  private static final boolean EXTRA_DEBUG = false;
  private static final int OUTPUT_BATCH_SIZE = 32*1024;

  private IterOutcome lastOutcome = null;
  private boolean first = true;
  private boolean newSchema = false;
  private int previousIndex = -1;
  private int underlyingIndex = 0;
  private int currentIndex;
  private long addedRecordCount = 0;
  private IterOutcome outcome;
  private int outputCount = 0;
  private RecordBatch incoming;
  private StreamingAggBatch outgoing;
  private boolean done = false;
  private OperatorContext context;


  @Override
  public void setup(OperatorContext context, RecordBatch incoming, StreamingAggBatch outgoing) throws SchemaChangeException {
    this.context = context;
    this.incoming = incoming;
    this.outgoing = outgoing;
    setupInterior(incoming, outgoing);
  }


  private void allocateOutgoing() {
    for (VectorWrapper<?> w : outgoing) {
      w.getValueVector().allocateNew();
    }
  }

  @Override
  public IterOutcome getOutcome() {
    return outcome;
  }

  @Override
  public int getOutputCount() {
    return outputCount;
  }

  @Override
  public AggOutcome doWork() {
    if (done) {
      outcome = IterOutcome.NONE;
      return AggOutcome.CLEANUP_AND_RETURN;
    }
    try { // outside loop to ensure that first is set to false after the first run.
      outputCount = 0;
      // allocate outgoing since either this is the first time or if a subsequent time we would
      // have sent the previous outgoing batch to downstream operator
      allocateOutgoing();

      if (first) {
        this.currentIndex = incoming.getRecordCount() == 0 ? 0 : this.getVectorIndex(underlyingIndex);

        // consume empty batches until we get one with data.
        if (incoming.getRecordCount() == 0) {
          outer: while (true) {
            IterOutcome out = outgoing.next(0, incoming);
            switch (out) {
            case OK_NEW_SCHEMA:
            case OK:
              if (incoming.getRecordCount() == 0) {
                continue;
              } else {
                currentIndex = this.getVectorIndex(underlyingIndex);
                break outer;
              }
            case OUT_OF_MEMORY:
              outcome = out;
              return AggOutcome.RETURN_OUTCOME;
            case NONE:
              out = IterOutcome.OK_NEW_SCHEMA;
            case STOP:
            default:
              lastOutcome = out;
              outcome = out;
              done = true;
              return AggOutcome.CLEANUP_AND_RETURN;
            }
          }
        }
      }


      if (newSchema) {
        return AggOutcome.UPDATE_AGGREGATOR;
      }

      if (lastOutcome != null) {
        outcome = lastOutcome;
        return AggOutcome.CLEANUP_AND_RETURN;
      }

      outside: while(true) {
      // loop through existing records, adding as necessary.
        for (; underlyingIndex < incoming.getRecordCount(); incIndex()) {
          if (EXTRA_DEBUG) {
            logger.debug("Doing loop with values underlying {}, current {}", underlyingIndex, currentIndex);
          }
          if (previousIndex == -1) {
            if (EXTRA_DEBUG) {
              logger.debug("Adding the initial row's keys and values.");
            }
            addRecordInc(currentIndex);
          }
          else if (isSame( previousIndex, currentIndex )) {
            if (EXTRA_DEBUG) {
              logger.debug("Values were found the same, adding.");
            }
            addRecordInc(currentIndex);
          } else {
            if (EXTRA_DEBUG) {
              logger.debug("Values were different, outputting previous batch.");
            }
            if(!outputToBatch(previousIndex)) {
              // There is still space in outgoing container, so proceed to the next input.
              if (EXTRA_DEBUG) {
                logger.debug("Output successful.");
              }
              addRecordInc(currentIndex);
            } else {
              if (EXTRA_DEBUG) {
                logger.debug("Output container has reached its capacity. Flushing it.");
              }

              // Update the indices to set the state for processing next record in incoming batch in subsequent doWork calls.
              previousIndex = -1;
              return setOkAndReturn();
            }
          }
          previousIndex = currentIndex;
        }

        InternalBatch previous = new InternalBatch(incoming, context);

        try {
          while (true) {

            IterOutcome out = outgoing.next(0, incoming);
            if (EXTRA_DEBUG) {
              logger.debug("Received IterOutcome of {}", out);
            }
            switch (out) {
            case NONE:
              done = true;
              lastOutcome = out;
              if (first && addedRecordCount == 0) {
                return setOkAndReturn();
              } else if(addedRecordCount > 0) {
                outputToBatchPrev(previous, previousIndex, outputCount); // No need to check the return value
                // (output container full or not) as we are not going to insert anymore records.
                if (EXTRA_DEBUG) {
                  logger.debug("Received no more batches, returning.");
                }
                return setOkAndReturn();
              }else{
                if (first && out == IterOutcome.OK) {
                  out = IterOutcome.OK_NEW_SCHEMA;
                }
                outcome = out;
                return AggOutcome.CLEANUP_AND_RETURN;
              }

            case NOT_YET:
              this.outcome = out;
              return AggOutcome.RETURN_OUTCOME;

            case OK_NEW_SCHEMA:
              if (EXTRA_DEBUG) {
                logger.debug("Received new schema.  Batch has {} records.", incoming.getRecordCount());
              }
              if (addedRecordCount > 0) {
                outputToBatchPrev(previous, previousIndex, outputCount); // No need to check the return value
                // (output container full or not) as we are not going to insert anymore records.
                if (EXTRA_DEBUG) {
                  logger.debug("Wrote out end of previous batch, returning.");
                }
                newSchema = true;
                return setOkAndReturn();
              }
              cleanup();
              return AggOutcome.UPDATE_AGGREGATOR;
            case OK:
              resetIndex();
              if (incoming.getRecordCount() == 0) {
                continue;
              } else {
                if (previousIndex != -1 && isSamePrev(previousIndex , previous, currentIndex)) {
                  if (EXTRA_DEBUG) {
                    logger.debug("New value was same as last value of previous batch, adding.");
                  }
                  addRecordInc(currentIndex);
                  previousIndex = currentIndex;
                  incIndex();
                  if (EXTRA_DEBUG) {
                    logger.debug("Continuing outside");
                  }
                  continue outside;
                } else { // not the same
                  if (EXTRA_DEBUG) {
                    logger.debug("This is not the same as the previous, add record and continue outside.");
                  }
                  if (addedRecordCount > 0) {
                    if (outputToBatchPrev(previous, previousIndex, outputCount)) {
                      if (EXTRA_DEBUG) {
                        logger.debug("Output container is full. flushing it.");
                      }
                      previousIndex = -1;
                      return setOkAndReturn();
                    }
                  }
                  previousIndex = -1;
                  continue outside;
                }
              }
            case STOP:
            default:
              lastOutcome = out;
              outcome = out;
              return AggOutcome.CLEANUP_AND_RETURN;
            }
          }
        } finally {
          // make sure to clear previous
          if (previous != null) {
            previous.clear();
          }
        }
      }
    } finally {
      if (first) {
        first = !first;
      }
    }

  }

  private final void incIndex() {
    underlyingIndex++;
    if (underlyingIndex >= incoming.getRecordCount()) {
      currentIndex = Integer.MAX_VALUE;
      return;
    }
    currentIndex = getVectorIndex(underlyingIndex);
  }

  private final void resetIndex() {
    underlyingIndex = -1;
    incIndex();
  }

  private final AggOutcome setOkAndReturn() {
    if (first) {
      this.outcome = IterOutcome.OK_NEW_SCHEMA;
    } else {
      this.outcome = IterOutcome.OK;
    }
    for (VectorWrapper<?> v : outgoing) {
      v.getValueVector().getMutator().setValueCount(outputCount);
    }
    return AggOutcome.RETURN_OUTCOME;
  }

  // Returns output container status after insertion of the given record. Caller must check the return value if it
  // plans to insert more records into outgoing container.
  private final boolean outputToBatch(int inIndex) {
    assert outputCount < OUTPUT_BATCH_SIZE:
        "Outgoing RecordBatch is not flushed. It reached its max capacity in the last update";

    outputRecordKeys(inIndex, outputCount);

    outputRecordValues(outputCount);

    if (EXTRA_DEBUG) {
      logger.debug("{} values output successfully", outputCount);
    }
    resetValues();
    outputCount++;
    addedRecordCount = 0;

    return outputCount == OUTPUT_BATCH_SIZE;
  }

  // Returns output container status after insertion of the given record. Caller must check the return value if it
  // plans to inserts more record into outgoing container.
  private final boolean outputToBatchPrev(InternalBatch b1, int inIndex, int outIndex) {
    assert outputCount < OUTPUT_BATCH_SIZE:
        "Outgoing RecordBatch is not flushed. It reached its max capacity in the last update";

    outputRecordKeysPrev(b1, inIndex, outIndex);
    outputRecordValues(outIndex);
    resetValues();
    outputCount++;
    addedRecordCount = 0;

    return outputCount == OUTPUT_BATCH_SIZE;
  }

  private void addRecordInc(int index) {
    addRecord(index);
    this.addedRecordCount++;
  }

  @Override
  public void cleanup() {
  }

  public abstract void setupInterior(@Named("incoming") RecordBatch incoming, @Named("outgoing") RecordBatch outgoing) throws SchemaChangeException;
  public abstract boolean isSame(@Named("index1") int index1, @Named("index2") int index2);
  public abstract boolean isSamePrev(@Named("b1Index") int b1Index, @Named("b1") InternalBatch b1, @Named("b2Index") int b2Index);
  public abstract void addRecord(@Named("index") int index);
  public abstract void outputRecordKeys(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);
  public abstract void outputRecordKeysPrev(@Named("previous") InternalBatch previous, @Named("previousIndex") int previousIndex, @Named("outIndex") int outIndex);
  public abstract void outputRecordValues(@Named("outIndex") int outIndex);
  public abstract int getVectorIndex(@Named("recordIndex") int recordIndex);
  public abstract boolean resetValues();

}
