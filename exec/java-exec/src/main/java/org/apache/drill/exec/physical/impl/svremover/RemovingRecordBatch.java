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
package org.apache.drill.exec.physical.impl.svremover;

import java.io.IOException;
import java.util.List;

import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.SelectionVectorRemover;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.vector.CopyUtil;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class RemovingRecordBatch extends AbstractSingleRecordBatch<SelectionVectorRemover>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RemovingRecordBatch.class);

  private Copier copier;
  private int recordCount;
  private boolean hasRemainder;
  private int remainderIndex;
  private boolean first;

  public RemovingRecordBatch(SelectionVectorRemover popConfig, FragmentContext context, RecordBatch incoming) throws OutOfMemoryException {
    super(popConfig, context, incoming);
    logger.debug("Created.");
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }

  @Override
  protected void setupNewSchema() throws SchemaChangeException {
    container.clear();

    switch(incoming.getSchema().getSelectionVectorMode()){
    case NONE:
      this.copier = getStraightCopier();
      break;
    case TWO_BYTE:
      this.copier = getGenerated2Copier();
      break;
    case FOUR_BYTE:
      this.copier = getGenerated4Copier();
      break;
    default:
      throw new UnsupportedOperationException();
    }

    container.buildSchema(SelectionVectorMode.NONE);

  }

  @Override
  public IterOutcome innerNext() {
    if (hasRemainder) {
      handleRemainder();
      return IterOutcome.OK;
    }
    return super.innerNext();
  }

  @Override
  protected IterOutcome doWork() {
    int incomingRecordCount = incoming.getRecordCount();
    int copiedRecords = copier.copyRecords(0, incomingRecordCount);

    if (copiedRecords < incomingRecordCount) {
      for(VectorWrapper<?> v : container){
        ValueVector.Mutator m = v.getValueVector().getMutator();
        m.setValueCount(copiedRecords);
      }
      hasRemainder = true;
      remainderIndex = copiedRecords;
      this.recordCount = remainderIndex;
    } else {
      recordCount = copiedRecords;
      for(VectorWrapper<?> v : container){
        ValueVector.Mutator m = v.getValueVector().getMutator();
        m.setValueCount(recordCount);
      }
      if (incoming.getSchema().getSelectionVectorMode() != SelectionVectorMode.FOUR_BYTE) {
        for(VectorWrapper<?> v: incoming) {
          v.clear();
        }
        if (incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.TWO_BYTE) {
          incoming.getSelectionVector2().clear();
        }
      }
    }

    assert recordCount >= copiedRecords;
    logger.debug("doWork(): {} records copied out of {}, remaining: {}, incoming schema {} ",
        copiedRecords,
        incomingRecordCount,
        incomingRecordCount - remainderIndex,
        incoming.getSchema());
    return IterOutcome.OK;
  }

  private void handleRemainder() {
    int recordCount = incoming.getRecordCount();
    int remainingRecordCount = incoming.getRecordCount() - remainderIndex;
    int copiedRecords;
    while((copiedRecords = copier.copyRecords(remainderIndex, remainingRecordCount)) == 0) {
      logger.debug("Copied zero records. Retrying");
      container.zeroVectors();
    }

    /*
    StringBuilder builder = new StringBuilder();
    for (VectorWrapper w : container) {
      builder.append(w.getField().getPath());
      builder.append(" Value capacity: ");
      builder.append(w.getValueVector().getValueCapacity());
      if (w.getValueVector() instanceof VariableWidthVector) {
        builder.append(" Byte capacity: ");
        builder.append(((VariableWidthVector) w.getValueVector()).getByteCapacity());
        builder.append("\n");
      }
    }
    logger.debug(builder.toString());
    */

    if (copiedRecords < remainingRecordCount) {
      for(VectorWrapper<?> v : container){
        ValueVector.Mutator m = v.getValueVector().getMutator();
        m.setValueCount(copiedRecords);
      }
      remainderIndex += copiedRecords;
      this.recordCount = copiedRecords;
    } else {
      for(VectorWrapper<?> v : container){
        ValueVector.Mutator m = v.getValueVector().getMutator();
        m.setValueCount(remainingRecordCount);
        this.recordCount = remainingRecordCount;
      }
      if (incoming.getSchema().getSelectionVectorMode() != SelectionVectorMode.FOUR_BYTE) {
        for(VectorWrapper<?> v: incoming) {
          v.clear();
        }
      }
      remainderIndex = 0;
      hasRemainder = false;
    }
    logger.debug(String.format("handleRemainder(): %s records copied out of %s, remaining: %s, incoming schema %s ",
        copiedRecords,
        recordCount,
        recordCount - remainderIndex,
        incoming.getSchema()));
  }

  @Override
  public void cleanup(){
    super.cleanup();
  }

  private class StraightCopier implements Copier{

    private List<TransferPair> pairs = Lists.newArrayList();
    private List<ValueVector> out = Lists.newArrayList();

    @Override
    public void setupRemover(FragmentContext context, RecordBatch incoming, RecordBatch outgoing){
      for(VectorWrapper<?> vv : incoming){
        TransferPair tp = vv.getValueVector().getTransferPair();
        pairs.add(tp);
        out.add(tp.getTo());
      }
    }

    @Override
    public int copyRecords(int index, int recordCount) {
      assert index == 0 && recordCount == incoming.getRecordCount() : "Straight copier cannot split batch";
      for(TransferPair tp : pairs){
        tp.transfer();
      }
      return recordCount;
    }

    public List<ValueVector> getOut() {
      return out;
    }

  }

  private Copier getStraightCopier(){
    StraightCopier copier = new StraightCopier();
    copier.setupRemover(context, incoming, this);
    container.addCollection(copier.getOut());
    return copier;
  }

  private Copier getGenerated2Copier() throws SchemaChangeException{
    Preconditions.checkArgument(incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.TWO_BYTE);

    List<ValueVector> out = Lists.newArrayList();
    for(VectorWrapper<?> vv : incoming){
      TransferPair tp = vv.getValueVector().getTransferPair();
      out.add(tp.getTo());
    }
    container.addCollection(out);

    try {
      final CodeGenerator<Copier> cg = CodeGenerator.get(Copier.TEMPLATE_DEFINITION2, context.getFunctionRegistry());
      CopyUtil.generateCopies(cg.getRoot(), incoming, false);
      Copier copier = context.getImplementationClass(cg);
      copier.setupRemover(context, incoming, this);

      return copier;
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
  }

  private Copier getGenerated4Copier() throws SchemaChangeException {
    Preconditions.checkArgument(incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.FOUR_BYTE);
    return getGenerated4Copier(incoming, context, oContext.getAllocator(), container, this);
  }

  public static Copier getGenerated4Copier(RecordBatch batch, FragmentContext context, BufferAllocator allocator, VectorContainer container, RecordBatch outgoing) throws SchemaChangeException{

    List<ValueVector> out = Lists.newArrayList();

    for(VectorWrapper<?> vv : batch){
      ValueVector v = vv.getValueVectors()[0];
      TransferPair tp = v.getTransferPair();
      out.add(tp.getTo());
    }
    container.addCollection(out);

    try {
      final CodeGenerator<Copier> cg = CodeGenerator.get(Copier.TEMPLATE_DEFINITION4, context.getFunctionRegistry());
      CopyUtil.generateCopies(cg.getRoot(), batch, true);
      Copier copier = context.getImplementationClass(cg);
      copier.setupRemover(context, batch, outgoing);

      return copier;
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
  }

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }



}
