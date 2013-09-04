package org.apache.drill.exec.physical.impl.sort;

import java.util.List;

import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.Lists;

/**
 * Holds the data for a particular record batch for later manipulation.
 */
public class RecordBatchData {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RecordBatchData.class);
  
  final List<ValueVector> vectors = Lists.newArrayList();
  final SelectionVector2 sv2;
  final int recordCount;
  
  public RecordBatchData(RecordBatch batch){
    this.sv2 = batch.getSchema().getSelectionVectorMode() == SelectionVectorMode.TWO_BYTE ? batch.getSelectionVector2().clone() : null;
    
    for(VectorWrapper<?> v : batch){
      if(v.isHyper()) throw new UnsupportedOperationException("Record batch data can't be created based on a hyper batch.");
      TransferPair tp = v.getValueVector().getTransferPair();
      tp.transfer();
      vectors.add(tp.getTo());
    }
    
    recordCount = batch.getRecordCount();
  }
  
  public int getRecordCount(){
    return recordCount;
  }
  public List<ValueVector> getVectors() {
    return vectors;
  }

  public SelectionVector2 getSv2() {
    return sv2;
  }
}
