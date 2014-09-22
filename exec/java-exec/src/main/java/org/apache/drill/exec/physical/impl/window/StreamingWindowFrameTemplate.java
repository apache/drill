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

package org.apache.drill.exec.physical.impl.window;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.aggregate.InternalBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorWrapper;

import javax.inject.Named;

public abstract class StreamingWindowFrameTemplate implements StreamingWindowFramer {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StreamingWindowFramer.class);
  private static final String TOO_BIG_ERROR = "Couldn't add value to an empty batch. This likely means that a single value is too long for a varlen field.";
  private static final boolean EXTRA_DEBUG = false;
  public static final int UNBOUNDED = -1;
  public static final int CURRENT_ROW = 0;
  private boolean first = true;
  private int previousIndex = 0;
  private int underlyingIndex = -1;
  private int currentIndex;
  private boolean pendingOutput = false;
  private RecordBatch.IterOutcome outcome;
  private int outputCount = 0;
  private RecordBatch incoming;
  private RecordBatch outgoing;
  private FragmentContext context;
  private InternalBatch previousBatch = null;
  private int precedingConfig = UNBOUNDED;
  private int followingConfig = CURRENT_ROW;


  @Override
  public void setup(FragmentContext context,
                    RecordBatch incoming,
                    RecordBatch outgoing,
                    int precedingConfig,
                    int followingConfig) throws SchemaChangeException {
    this.context = context;
    this.incoming = incoming;
    this.outgoing = outgoing;
    this.precedingConfig = precedingConfig;
    this.followingConfig = followingConfig;

    setupInterior(incoming, outgoing);
  }


  private void allocateOutgoing() {
    for (VectorWrapper<?> w : outgoing){
      w.getValueVector().allocateNew();
    }
  }

  @Override
  public RecordBatch.IterOutcome getOutcome() {
    return outcome;
  }

  @Override
  public int getOutputCount() {
    return outputCount;
  }

  private AggOutcome tooBigFailure() {
    context.fail(new Exception(TOO_BIG_ERROR));
    this.outcome = RecordBatch.IterOutcome.STOP;
    return AggOutcome.RETURN_OUTCOME;
  }

  @Override
  public AggOutcome doWork() {
    // if we're in the first state, allocate outgoing.
    try {
      if (first) {
        allocateOutgoing();
      }

      // setup for new output and pick any remainder.
      if (pendingOutput) {
        allocateOutgoing();
        pendingOutput = false;
        outputToBatch(previousIndex);
      }

      boolean recordsAdded = false;

      outside: while (true) {
        if (EXTRA_DEBUG) {
          logger.trace("Looping from underlying index {}, previous {}, current {}", underlyingIndex, previousIndex, currentIndex);
          logger.debug("Processing {} records in window framer", incoming.getRecordCount());
        }
        // loop through existing records, adding as necessary.
        while(incIndex()) {
          if (previousBatch != null) {
            boolean isSameFromBatch = isSameFromBatch(previousIndex, previousBatch, currentIndex);
            if (EXTRA_DEBUG) {
              logger.trace("Same as previous batch: {}, previous index {}, current index {}", isSameFromBatch, previousIndex, currentIndex);
            }

            if(!isSameFromBatch) {
              resetValues();
            }
            previousBatch.clear();
            previousBatch = null;
          } else if (!isSame(previousIndex, currentIndex)) {
            resetValues();
          }

          addRecord(currentIndex);

          if (!outputToBatch(currentIndex)) {
            if (outputCount == 0) {
              return tooBigFailure();
            }

            // mark the pending output but move forward for the next cycle.
            pendingOutput = true;
            incIndex();
            return setOkAndReturn();
          }

          recordsAdded = true;
        }

        if (EXTRA_DEBUG) {
          logger.debug("Exit Loop from underlying index {}, previous {}, current {}", underlyingIndex, previousIndex, currentIndex);
        }

        previousBatch = new InternalBatch(incoming);

        while (true) {
          RecordBatch.IterOutcome out = incoming.next();
          switch (out) {
            case NONE:
              outcome = innerOutcome(out, recordsAdded);
              if (EXTRA_DEBUG) {
                logger.trace("Received IterOutcome of {}, assigning {} outcome", out, outcome);
              }
              return AggOutcome.RETURN_AND_COMPLETE;
            case NOT_YET:
              outcome = innerOutcome(out, recordsAdded);
              if (EXTRA_DEBUG) {
                logger.trace("Received IterOutcome of {}, assigning {} outcome", out, outcome);
              }
              return AggOutcome.RETURN_OUTCOME;

            case OK_NEW_SCHEMA:
              if (EXTRA_DEBUG) {
                logger.trace("Received new schema.  Batch has {} records.", incoming.getRecordCount());
              }
              resetIndex();
              return AggOutcome.UPDATE_AGGREGATOR;

            case OK:
              if (EXTRA_DEBUG) {
                logger.trace("Received OK with {} records.", incoming.getRecordCount());
              }
              resetIndex();
              if (incoming.getRecordCount() == 0) {
                continue;
              } else {
                continue outside;
              }
            case STOP:
            default:
              outcome = out;
              if (EXTRA_DEBUG) {
                logger.trace("Stop received.", incoming.getRecordCount());
              }
              return AggOutcome.RETURN_OUTCOME;
          }
        }
      }
    } finally {
      first = false;
    }
  }

  private RecordBatch.IterOutcome innerOutcome(RecordBatch.IterOutcome innerOutcome, boolean newRecordsAdded) {
    if(newRecordsAdded) {
      setOkAndReturn();
      return outcome;
    }
    return innerOutcome;
  }


  private final boolean incIndex() {
    underlyingIndex++;

    if(currentIndex != -1) {
      previousIndex = currentIndex;
    }

    if (underlyingIndex >= incoming.getRecordCount()) {
      return false;
    }

    currentIndex = getVectorIndex(underlyingIndex);
    return true;
  }

  private final void resetIndex() {
    underlyingIndex = -1;
    currentIndex = getVectorIndex(underlyingIndex);
    if (EXTRA_DEBUG) {
      logger.trace("Reset new indexes: underlying {}, previous {}, current {}", underlyingIndex, previousIndex, currentIndex);
    }
  }

  private final AggOutcome setOkAndReturn() {
    if (first) {
      this.outcome = RecordBatch.IterOutcome.OK_NEW_SCHEMA;
    } else {
      this.outcome = RecordBatch.IterOutcome.OK;
    }

    if (EXTRA_DEBUG) {
      logger.debug("Setting output count {}", outputCount);
    }
    for (VectorWrapper<?> v : outgoing) {
      v.getValueVector().getMutator().setValueCount(outputCount);
    }
    return AggOutcome.RETURN_OUTCOME;
  }

  private final boolean outputToBatch(int inIndex) {
    boolean success = outputRecordValues(outputCount)
        && outputWindowValues(inIndex, outputCount);

    if (success) {
      outputCount++;
    }

    return success;
  }

  @Override
  public void cleanup() {
    if(previousBatch != null) {
      previousBatch.clear();
      previousBatch = null;
    }
  }

  public abstract void setupInterior(@Named("incoming") RecordBatch incoming, @Named("outgoing") RecordBatch outgoing) throws SchemaChangeException;

  /**
   * Compares withins from two indexes in the same batch
   * @param index1 First record index
   * @param index2 Second record index
   * @return does within value match
   */
  public abstract boolean isSame(@Named("index1") int index1, @Named("index2") int index2);
  /**
   * Compares withins from one index of given batch (typically previous completed batch), and one index from current batch
   * @param b1Index First record index
   * @param index2 Second record index
   * @return does within value match
   */
  public abstract boolean isSameFromBatch(@Named("b1Index") int b1Index, @Named("b1") InternalBatch b1, @Named("b2Index") int index2);
  public abstract void addRecord(@Named("index") int index);
  public abstract boolean outputRecordValues(@Named("outIndex") int outIndex);
  public abstract boolean outputWindowValues(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);
  public abstract int getVectorIndex(@Named("recordIndex") int recordIndex);

  public abstract boolean resetValues();
}
