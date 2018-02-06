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
package org.apache.drill.exec.physical.impl.svremover;

import java.util.List;

import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.SelectionVectorRemover;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class RemovingRecordBatch extends AbstractSingleRecordBatch<SelectionVectorRemover>{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RemovingRecordBatch.class);

  private Copier copier;

  public RemovingRecordBatch(SelectionVectorRemover popConfig, FragmentContext context, RecordBatch incoming) throws OutOfMemoryException {
    super(popConfig, context, incoming);
    logger.debug("Created.");
  }

  @Override
  public int getRecordCount() {
    return container.getRecordCount();
  }

  @Override
  protected boolean setupNewSchema() throws SchemaChangeException {
    container.clear();
    switch(incoming.getSchema().getSelectionVectorMode()){
    case NONE:
      this.copier = getStraightCopier();
      break;
    case TWO_BYTE:
      this.copier = create2Copier();
      break;
    case FOUR_BYTE:
      this.copier = create4Copier();
      break;
    default:
      throw new UnsupportedOperationException();
    }

    if (container.isSchemaChanged()) {
      container.buildSchema(SelectionVectorMode.NONE);
      return true;
    }

    return false;
  }

  @Override
  public IterOutcome innerNext() {
    return super.innerNext();
  }

  @Override
  protected IterOutcome doWork() {
    try {
      copier.copyRecords(0, incoming.getRecordCount());
    } catch (SchemaChangeException e) {
      throw new IllegalStateException(e);
    } finally {
      if (incoming.getSchema().getSelectionVectorMode() != SelectionVectorMode.FOUR_BYTE) {
        for(VectorWrapper<?> v: incoming) {
          v.clear();
        }
        if (incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.TWO_BYTE) {
          incoming.getSelectionVector2().clear();
        }
      }
    }

    logger.debug("doWork(): {} records copied out of {}, incoming schema {} ",
      container.getRecordCount(), container.getRecordCount(), incoming.getSchema());
    return IterOutcome.OK;
  }

  @Override
  public void close() {
    super.close();
  }

  private class StraightCopier implements Copier{

    private List<TransferPair> pairs = Lists.newArrayList();

    @Override
    public void setup(RecordBatch incoming, VectorContainer outgoing){
      for(VectorWrapper<?> vv : incoming){
        TransferPair tp = vv.getValueVector().makeTransferPair(container.addOrGet(vv.getField(), callBack));
        pairs.add(tp);
      }
    }

    @Override
    public int copyRecords(int index, int recordCount) {
      assert index == 0 && recordCount == incoming.getRecordCount() : "Straight copier cannot split batch";
      for(TransferPair tp : pairs){
        tp.transfer();
      }

      container.setRecordCount(incoming.getRecordCount());
      return recordCount;
    }

    @Override
    public int appendRecord(int index) throws SchemaChangeException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int appendRecords(int index, int recordCount) throws SchemaChangeException {
      throw new UnsupportedOperationException();
    }
  }

  private Copier getStraightCopier(){
    StraightCopier copier = new StraightCopier();
    copier.setup(incoming, container);
    return copier;
  }

  private Copier create2Copier() throws SchemaChangeException {
    Preconditions.checkArgument(incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.TWO_BYTE);

    for(VectorWrapper<?> vv : incoming){
      vv.getValueVector().makeTransferPair(container.addOrGet(vv.getField(), callBack));
    }

    Copier copier = new GenericSV2Copier();
    copier.setup(incoming, container);
    return copier;
  }

  private Copier create4Copier() throws SchemaChangeException {
    Preconditions.checkArgument(incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.FOUR_BYTE);
    return GenericSV4Copier.createCopier(incoming, container, callBack);
  }

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }
}
