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
package org.apache.drill.exec.physical.impl.limit;

import java.util.List;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Limit;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;

import com.google.common.collect.Lists;

public class LimitRecordBatch extends AbstractSingleRecordBatch<Limit> {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LimitRecordBatch.class);

  private SelectionVector2 outgoingSv;
  private SelectionVector2 incomingSv;
  private int recordsToSkip;
  private int recordsLeft;
  private boolean noEndLimit;
  private boolean skipBatch;
  private boolean first = true;
  private boolean done = false;
  List<TransferPair> transfers = Lists.newArrayList();

  public LimitRecordBatch(Limit popConfig, FragmentContext context, RecordBatch incoming) throws OutOfMemoryException {
    super(popConfig, context, incoming);
    outgoingSv = new SelectionVector2(oContext.getAllocator());
    recordsToSkip = popConfig.getFirst();
    noEndLimit = popConfig.getLast() == null;
    if(!noEndLimit) {
      recordsLeft = popConfig.getLast() - recordsToSkip;
    }
    skipBatch = false;
  }

  @Override
  protected void setupNewSchema() throws SchemaChangeException {
    container.clear();


    for(VectorWrapper<?> v : incoming){
      TransferPair pair = v.getValueVector().getTransferPair();
      container.add(pair.getTo());
      transfers.add(pair);
    }

    BatchSchema.SelectionVectorMode svMode = incoming.getSchema().getSelectionVectorMode();

    switch(svMode){
      case NONE:
        break;
      case TWO_BYTE:
        this.incomingSv = incoming.getSelectionVector2();
        break;
      default:
        throw new UnsupportedOperationException();
    }

    container.buildSchema(BatchSchema.SelectionVectorMode.TWO_BYTE);

  }

  @Override
  public IterOutcome innerNext() {
    if (done) {
      return IterOutcome.NONE;
    }

    if(!noEndLimit && recordsLeft <= 0) {
      if (first) {
        return produceEmptyFirstBatch();
      }

      incoming.kill(true);

      IterOutcome upStream = incoming.next();

      while (upStream == IterOutcome.OK || upStream == IterOutcome.OK_NEW_SCHEMA) {

        // Clear the memory for the incoming batch
        for (VectorWrapper<?> wrapper : incoming) {
          wrapper.getValueVector().clear();
        }
        upStream = incoming.next();
      }

      first = false;
      return IterOutcome.NONE;
    }

    first = false;
    return super.innerNext();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    return outgoingSv;
  }

  @Override
  protected IterOutcome doWork() {
    for(TransferPair tp : transfers) {
      tp.transfer();
    }
    skipBatch = false;
    int recordCount = incoming.getRecordCount();
    if(recordCount <= recordsToSkip) {
      recordsToSkip -= recordCount;
      skipBatch = true;
    } else {
      outgoingSv.allocateNew(recordCount);
      if(incomingSv != null) {
       limitWithSV(recordCount);
      } else {
       limitWithNoSV(recordCount);
      }
    }

    return IterOutcome.OK;
  }

  private IterOutcome produceEmptyFirstBatch() {
    incoming.next();
    first = false;
    done = true;
    // Build the container schema and set the count
    for (VectorWrapper<?> v : incoming) {
      TransferPair pair = v.getValueVector().getTransferPair();
      container.add(pair.getTo());
      transfers.add(pair);
    }
    container.buildSchema(BatchSchema.SelectionVectorMode.TWO_BYTE);
    container.setRecordCount(0);

    incoming.kill(true);
    return IterOutcome.OK_NEW_SCHEMA;
  }

  private void limitWithNoSV(int recordCount) {
    int offset = Math.max(0, Math.min(recordCount - 1, recordsToSkip));
    recordsToSkip -= offset;
    int fetch;

    if(noEndLimit) {
      fetch = recordCount;
    } else {
      fetch = Math.min(recordCount, offset + recordsLeft);
      recordsLeft -= Math.max(0, fetch - offset);
    }

    int svIndex = 0;
    for(char i = (char) offset; i < fetch; i++) {
      outgoingSv.setIndex(svIndex, i);
      svIndex++;
    }
    outgoingSv.setRecordCount(svIndex);
  }

  private void limitWithSV(int recordCount) {
    int offset = Math.max(0, Math.min(recordCount - 1, recordsToSkip));
    recordsToSkip -= offset;
    int fetch;

    if(noEndLimit) {
      fetch = recordCount;
    } else {
      fetch = Math.min(recordCount, recordsLeft);
      recordsLeft -= Math.max(0, fetch - offset);
    }

    int svIndex = 0;
    for(int i = offset; i < fetch; i++) {
      char index = incomingSv.getIndex(i);
      outgoingSv.setIndex(svIndex, index);
      svIndex++;
    }
    outgoingSv.setRecordCount(svIndex);
  }

  @Override
  public int getRecordCount() {
    return skipBatch ? 0 : outgoingSv.getCount();
  }

  @Override
  public void cleanup(){
    outgoingSv.clear();
    super.cleanup();
  }

}
