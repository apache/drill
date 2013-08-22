package org.apache.drill.exec.physical.impl.union;

import com.google.common.collect.Lists;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Union;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.ValueVector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class UnionRecordBatch extends AbstractRecordBatch<Union> {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnionRecordBatch.class);

  private final List<RecordBatch> incoming;
  private SelectionVector2 sv;
  private Iterator<RecordBatch> incomingIterator = null;
  private RecordBatch current = null;
  private ArrayList<TransferPair> transfers;
  private int outRecordCount;

  public UnionRecordBatch(Union config, List<RecordBatch> children, FragmentContext context) {
    super(config, context);
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
  public void kill() {
    if(current != null){
      current.kill();
      current = null;
    }
    for(;incomingIterator.hasNext();){
      incomingIterator.next().kill();
    }
  }

  @Override
  protected void killIncoming() {
    for (int i = 0; i < incoming.size(); i++) {
      RecordBatch in = incoming.get(i);
      in.kill();
    }
  }


  @Override
  public SelectionVector2 getSelectionVector2() {
    return sv;
  }

  @Override
  public IterOutcome next() {
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
    if (container.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE) {
      this.sv = current.getSelectionVector2();
    }
    for (TransferPair transfer : transfers) {
      transfer.transfer();
    }

    for (VectorWrapper<?> vw : this.container) {
      ValueVector.Mutator m = vw.getValueVector().getMutator();
      m.setValueCount(outRecordCount);
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
}
