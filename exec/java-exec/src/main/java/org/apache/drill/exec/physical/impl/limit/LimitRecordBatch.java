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

  public LimitRecordBatch(Limit popConfig, FragmentContext context, RecordBatch incoming) {
    super(popConfig, context, incoming);
    outgoingSv = new SelectionVector2(context.getAllocator());
  }

  @Override
  protected void setupNewSchema() throws SchemaChangeException {
    container.clear();

    List<TransferPair> transfers = Lists.newArrayList();

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


    for(TransferPair tp : transfers) {
      tp.transfer();
    }
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    return outgoingSv;
  }

  @Override
  protected void doWork() {
    int recordCount = incoming.getRecordCount();
    outgoingSv.allocateNew(recordCount);

    if(incomingSv != null) {
      limitWithSV(recordCount);
    } else {
      limitWithNoSV(recordCount);
    }
  }

  private void limitWithNoSV(int recordCount) {
    int svIndex = 0;
    int offset = Math.max(0, Objects.firstNonNull(popConfig.getFirst(), 0));
    int fetch = Math.min(recordCount, Objects.firstNonNull(popConfig.getLast(), recordCount));
    for(char i = (char) offset; i < fetch; i++) {
      outgoingSv.setIndex(svIndex, i);
      svIndex++;
    }
    outgoingSv.setRecordCount(svIndex);
  }

  private void limitWithSV(int recordCount) {
    int svIndex = 0;
    int offset = Math.max(0, Objects.firstNonNull(popConfig.getFirst(), 0));
    int fetch = Math.min(recordCount, Objects.firstNonNull(popConfig.getLast(), recordCount));
    for(int i = offset; i < fetch; i++) {
      char index = incomingSv.getIndex(i);
      outgoingSv.setIndex(svIndex, index);
      svIndex++;
    }

    outgoingSv.setRecordCount(svIndex);
  }

  @Override
  public int getRecordCount() {
    return outgoingSv.getCount();
  }

  @Override
  protected void cleanup(){
    super.cleanup();
    outgoingSv.clear();
  }
}
