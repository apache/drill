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
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.allocator.VectorAllocator;

public abstract class StreamingAggTemplate implements StreamingAggregator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StreamingAggregator.class);
  private static final boolean EXTRA_DEBUG = false;
  private static final String TOO_BIG_ERROR = "Couldn't add value to an empty batch.  This likely means that a single value is too long for a varlen field.";
  private IterOutcome lastOutcome = null;
  private boolean first = true;
  private boolean newSchema = false;
  private int previousIndex = 0;
  private int underlyingIndex = 0;
  private int currentIndex;
  private int addedRecordCount = 0;
  private boolean pendingOutput = false;
  private IterOutcome outcome;
  private int outputCount = 0;
  private RecordBatch incoming;
  private BatchSchema schema;
  private RecordBatch outgoing;
  private VectorAllocator[] allocators;
  private FragmentContext context;
  private InternalBatch remainderBatch;


  @Override
  public void setup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing, VectorAllocator[] allocators) throws SchemaChangeException {
    this.allocators = allocators;
    this.context = context;
    this.incoming = incoming;
    this.schema = incoming.getSchema();
    this.allocators = allocators;
    this.outgoing = outgoing;
    setupInterior(incoming, outgoing);
    this.currentIndex = this.getVectorIndex(underlyingIndex);
  }

  
  private void allocateOutgoing() {
    for (VectorAllocator a : allocators) {
      if(EXTRA_DEBUG) logger.debug("Allocating {} with {} records.", a, 20000);
      a.alloc(20000);
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

  private AggOutcome tooBigFailure(){
    context.fail(new Exception(TOO_BIG_ERROR));
    this.outcome = IterOutcome.STOP;
    return AggOutcome.CLEANUP_AND_RETURN;
  }
  
  @Override
  public AggOutcome doWork() {
    try{ // outside loop to ensure that first is set to false after the first run.
      
      // if we're in the first state, allocate outgoing.
      if(first){
        allocateOutgoing();
      }
      
      // pick up a remainder batch if we have one.
      if(remainderBatch != null){
        if (!outputToBatch( previousIndex )) return tooBigFailure();
        remainderBatch.clear();
        remainderBatch = null;
        return setOkAndReturn();
      }
      
      
      // setup for new output and pick any remainder.
      if (pendingOutput) {
        allocateOutgoing();
        pendingOutput = false;
        if (!outputToBatch( previousIndex)) return tooBigFailure();
      }
  
      if(newSchema){
        return AggOutcome.UPDATE_AGGREGATOR;
      }
      
      if(lastOutcome != null){
        outcome = lastOutcome;
        return AggOutcome.CLEANUP_AND_RETURN;
      }
      
      outside: while(true){
      // loop through existing records, adding as necessary.
        for (; underlyingIndex < incoming.getRecordCount(); incIndex()) {
          if(EXTRA_DEBUG) logger.debug("Doing loop with values underlying {}, current {}", underlyingIndex, currentIndex);
          if (isSame( previousIndex, currentIndex )) {
            if(EXTRA_DEBUG) logger.debug("Values were found the same, adding.");
            addRecordInc(currentIndex);
          } else {
            if(EXTRA_DEBUG) logger.debug("Values were different, outputting previous batch.");
            if (outputToBatch(previousIndex)) {
              if(EXTRA_DEBUG) logger.debug("Output successful.");
              addRecordInc(currentIndex);
            } else {
              if(EXTRA_DEBUG) logger.debug("Output failed.");
              if(outputCount == 0) return tooBigFailure();
              
              // mark the pending output but move forward for the next cycle.
              pendingOutput = true;
              previousIndex = currentIndex;
              incIndex();
              return setOkAndReturn();
              
            }
          }
          previousIndex = currentIndex;
        }
        
        
        InternalBatch previous = null;
        
        try{
          while(true){
            previous = new InternalBatch(incoming);
            IterOutcome out = incoming.next();
            if(EXTRA_DEBUG) logger.debug("Received IterOutcome of {}", out);
            switch(out){
            case NONE:
              lastOutcome = out;
              if(addedRecordCount > 0){
                if( !outputToBatchPrev( previous, previousIndex, outputCount) ) remainderBatch = previous;
                if(EXTRA_DEBUG) logger.debug("Received no more batches, returning.");
                return setOkAndReturn();
              }else{
                outcome = out;
                return AggOutcome.CLEANUP_AND_RETURN;
              }
              

              
            case NOT_YET:
              this.outcome = out;
              return AggOutcome.RETURN_OUTCOME;
              
            case OK_NEW_SCHEMA:
              if(EXTRA_DEBUG) logger.debug("Received new schema.  Batch has {} records.", incoming.getRecordCount());
              if(addedRecordCount > 0){
                if( !outputToBatchPrev( previous, previousIndex, outputCount) ) remainderBatch = previous;
                if(EXTRA_DEBUG) logger.debug("Wrote out end of previous batch, returning.");
                newSchema = true;
                return setOkAndReturn();
              }
              cleanup();
              return AggOutcome.UPDATE_AGGREGATOR;   
            case OK:
              resetIndex();
              if(incoming.getRecordCount() == 0){
                continue;
              }else{
                if(isSamePrev(previousIndex , previous, currentIndex)){
                  if(EXTRA_DEBUG) logger.debug("New value was same as last value of previous batch, adding.");
                  addRecordInc(currentIndex);
                  previousIndex = currentIndex;
                  incIndex();
                  if(EXTRA_DEBUG) logger.debug("Continuing outside");
                  continue outside;
                }else{ // not the same
                  if(EXTRA_DEBUG) logger.debug("This is not the same as the previous, add record and continue outside.");
                  previousIndex = currentIndex;
                  if(addedRecordCount > 0){
                    if( !outputToBatchPrev( previous, previousIndex, outputCount) ){
                      remainderBatch = previous;
                      return setOkAndReturn(); 
                    }
                    continue outside;
                  }
                }
              }
            case STOP:
            default:
              lastOutcome = out;
              outcome = out;
              return AggOutcome.CLEANUP_AND_RETURN;
            }


        }
        }finally{
          // make sure to clear previous if we haven't saved it.
          if(remainderBatch == null && previous != null){
            previous.clear();
          }
        }
      }
    }finally{
      if(first) first = !first;
    }
    
  }
  
  
  private final void incIndex(){
    underlyingIndex++;
    if(underlyingIndex >= incoming.getRecordCount()){
      currentIndex = Integer.MAX_VALUE;
      return;
    }
    currentIndex = getVectorIndex(underlyingIndex);
  }
  
  private final void resetIndex(){
    underlyingIndex = -1;
    incIndex();
  }
  
  private final AggOutcome setOkAndReturn(){
    if(first){
      this.outcome = IterOutcome.OK_NEW_SCHEMA;
    }else{
      this.outcome = IterOutcome.OK;
    }
    for(VectorWrapper<?> v : outgoing){
      v.getValueVector().getMutator().setValueCount(outputCount);
    }
    return AggOutcome.RETURN_OUTCOME;
  }

  private final boolean outputToBatch(int inIndex){
    boolean success = outputRecordKeys(inIndex, outputCount) //
        && outputRecordValues(outputCount) //
        && resetValues();
    if(success){
      if(EXTRA_DEBUG) logger.debug("Outputting values to {}", outputCount);
      outputCount++;
      addedRecordCount = 0;
    }
    
    return success;
  }

  private final boolean outputToBatchPrev(InternalBatch b1, int inIndex, int outIndex){
    boolean success = outputRecordKeysPrev(b1, inIndex, outIndex) //
        && outputRecordValues(outIndex) //
        && resetValues();
    if(success){
      outputCount++;
      addedRecordCount = 0;
    }
    
    return success;
  }
  
  private void addRecordInc(int index){
    addRecord(index);
    this.addedRecordCount++;
  }
  
  @Override
  public void cleanup(){
    if(remainderBatch != null) remainderBatch.clear(); 
  }


  public abstract void setupInterior(@Named("incoming") RecordBatch incoming, @Named("outgoing") RecordBatch outgoing) throws SchemaChangeException;
  public abstract boolean isSame(@Named("index1") int index1, @Named("index2") int index2);
  public abstract boolean isSamePrev(@Named("b1Index") int b1Index, @Named("b1") InternalBatch b1, @Named("b2Index") int b2Index);
  public abstract void addRecord(@Named("index") int index);
  public abstract boolean outputRecordKeys(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);
  public abstract boolean outputRecordKeysPrev(@Named("previous") InternalBatch previous, @Named("previousIndex") int previousIndex, @Named("outIndex") int outIndex);
  public abstract boolean outputRecordValues(@Named("outIndex") int outIndex);
  public abstract int getVectorIndex(@Named("recordIndex") int recordIndex);
  public abstract boolean resetValues();
  
}
