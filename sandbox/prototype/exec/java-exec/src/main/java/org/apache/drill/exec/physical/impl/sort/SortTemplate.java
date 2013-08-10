package org.apache.drill.exec.physical.impl.sort;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

public abstract class SortTemplate implements Sorter, IndexedSortable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SortTemplate.class);
  
  private SelectionVector4 vector4;
  
  
  public void setup(FragmentContext context, RecordBatch hyperBatch) throws SchemaChangeException{
    // we pass in the local hyperBatch since that is where we'll be reading data.
    vector4 = hyperBatch.getSelectionVector4();
    doSetup(context, hyperBatch, null);
  }
  
  @Override
  public void sort(SelectionVector4 vector4, VectorContainer container){
    QuickSort qs = new QuickSort();
    qs.sort(this, 0, vector4.getTotalCount());
  }

  @Override
  public void swap(int sv0, int sv1) {
    int tmp = vector4.get(sv0);
    vector4.set(sv0, vector4.get(sv1));
    vector4.set(sv1, tmp);
  }
  
  @Override
  public int compare(int leftIndex, int rightIndex) {
    int sv1 = vector4.get(leftIndex);
    int sv2 = vector4.get(rightIndex);
    return doEval(sv1, sv2);
  }

  public abstract void doSetup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing) throws SchemaChangeException;
  public abstract int doEval(int leftIndex, int rightIndex);
}
