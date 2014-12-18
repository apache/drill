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
package org.apache.drill.exec.physical.impl.union;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.UnionAll;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;

import com.google.common.collect.Lists;

public class UnionAllRecordBatch extends AbstractRecordBatch<UnionAll> {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnionAllRecordBatch.class);

  private final List<RecordBatch> incoming;
  private SelectionVector2 sv;
  private Iterator<RecordBatch> incomingIterator = null;
  private RecordBatch current = null;
  private ArrayList<TransferPair> transfers;
  private int outRecordCount;

  public UnionAllRecordBatch(UnionAll config, List<RecordBatch> children, FragmentContext context) throws OutOfMemoryException {
    super(config, context, false);
    this.incoming = children;
    this.incomingIterator = incoming.iterator();
    current = incomingIterator.next();
    sv = null;
  }

  @Override
  public int getRecordCount() {
    return outRecordCount;
  }

  @Override
  public void kill(boolean sendUpstream) {
    if(current != null){
      current.kill(sendUpstream);
      current = null;
    }
    for(;incomingIterator.hasNext();){
      incomingIterator.next().kill(sendUpstream);
    }
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    for (int i = 0; i < incoming.size(); i++) {
      RecordBatch in = incoming.get(i);
      in.kill(sendUpstream);
    }
  }

  public void buildSchema() throws SchemaChangeException {
    setupSchema();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    return sv;
  }

  @Override
  public IterOutcome innerNext() {
    if (current == null) { // end of iteration
      return IterOutcome.NONE;
    }
    IterOutcome upstream = current.next();
    logger.debug("Upstream... {}", upstream);
    while (upstream == IterOutcome.NONE) {
      if (!incomingIterator.hasNext()) {
        current = null;
        return IterOutcome.NONE;
      }
      current = incomingIterator.next();
      upstream = current.next();
      if (upstream == IterOutcome.OK) {
        upstream = IterOutcome.OK_NEW_SCHEMA;
      }
    }
    switch (upstream) {
      case NONE:
        throw new IllegalArgumentException("not possible!");
      case NOT_YET:
      case STOP:
        return upstream;
      case OK_NEW_SCHEMA:
        setupSchema();
        // fall through.
      case OK:
        doTransfer();
        return upstream; // change if upstream changed, otherwise normal.
      default:
        throw new UnsupportedOperationException();
    }
  }

  private void doTransfer() {
    outRecordCount = current.getRecordCount();
    // If the batch is empty we still need to set up the outgoing vectors otherwise the downstream operators will get
    // a NPE. SEE DRILL-1886
    if (container.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE) {
      this.sv = current.getSelectionVector2();
    }
    for (TransferPair transfer : transfers) {
      transfer.transfer();
    }
  }

  private void setupSchema() {
    if (container != null) {
      container.clear();
    }
    transfers = Lists.newArrayList();

    for (VectorWrapper<?> vw : current) {
      TransferPair pair = vw.getValueVector().getTransferPair();
      container.add(pair.getTo());
      transfers.add(pair);
    }
    container.buildSchema(current.getSchema().getSelectionVectorMode());
  }

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }

  @Override
  public void cleanup() {
    super.cleanup();
    for (int i = 0; i < incoming.size(); i++) {
      RecordBatch in = incoming.get(i);
      in.cleanup();
    }
  }

}
