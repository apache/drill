package org.apache.drill.exec.physical.impl.filter;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.allocator.VectorAllocator;

import javax.inject.Named;

public abstract class FilterTemplate4 implements Filterer {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FilterTemplate4.class);

  private SelectionVector4 outgoingSelectionVector;
  private SelectionVector4 incomingSelectionVector;
  private TransferPair[] transfers;

  @Override
  public void setup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing, TransferPair[] transfers)
      throws SchemaChangeException {
    this.transfers = transfers;
    this.outgoingSelectionVector = outgoing.getSelectionVector4();
    this.incomingSelectionVector = incoming.getSelectionVector4();
    doSetup(context, incoming, outgoing);
  }

  @Override
  public void filterBatch(int recordCount){
    int outPos = 0;
    for (int i = 0; i < incomingSelectionVector.getCount(); i++) {
      int index = incomingSelectionVector.get(i);
      if (doEval(index, 0)) {
        System.out.println(" (match): " + index + " (i: " + i + ") ");
        outgoingSelectionVector.set(outPos++, index);
      }
    }
    outgoingSelectionVector.setCount(outPos);
    doTransfers();
  }

  private void doTransfers(){
    for(TransferPair t : transfers){
      t.transfer();
    }
  }

  public abstract void doSetup(@Named("context") FragmentContext context, @Named("incoming") RecordBatch incoming, @Named("outgoing") RecordBatch outgoing);
  public abstract boolean doEval(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);

}
