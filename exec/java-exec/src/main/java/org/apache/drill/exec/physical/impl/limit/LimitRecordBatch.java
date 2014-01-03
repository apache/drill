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

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Objects;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Limit;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.ValueVector;

import java.util.List;

public class LimitRecordBatch extends AbstractSingleRecordBatch<Limit> {

  private SelectionVector2 outgoingSv;
  private SelectionVector2 incomingSv;
  private int recordsToSkip;
  private int recordsLeft;
  private boolean noEndLimit;
  private boolean skipBatch;
  List<TransferPair> transfers = Lists.newArrayList();

  public LimitRecordBatch(Limit popConfig, FragmentContext context, RecordBatch incoming) {
    super(popConfig, context, incoming);
    outgoingSv = new SelectionVector2(context.getAllocator());
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
  public IterOutcome next() {
    if(!noEndLimit && recordsLeft <= 0) {
      killIncoming();
      return IterOutcome.NONE;
    }

    return super.next();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    return outgoingSv;
  }

  @Override
  protected void doWork() {
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
    super.cleanup();
    outgoingSv.clear();
  }
}
