package org.apache.drill.exec.ref.rops;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.logical.data.Order;
import org.apache.drill.common.logical.data.Order.Direction;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.eval.EvaluatorFactory;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.ref.values.ComparableValue;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

public class OrderROP extends AbstractBlockingOperator<Order>{
  
  private List<CompoundValue> records = new ArrayList<CompoundValue>();
  private SortDefinition[] defs;
  
  public OrderROP(Order config) {
    super(config);
  }

  @Override
  protected void setupEvals(EvaluatorFactory builder) {
    Ordering[] orderings = config.getOrderings();
    defs = new SortDefinition[orderings.length];
    for(int i =0; i < orderings.length; i++){
      defs[i] = new SortDefinition(builder.getBasicEvaluator(inputRecord, orderings[i].getExpr()), orderings[i].getDirection() == Direction.ASC);
    }
  }

  
  @Override
  protected void consumeRecord() {
    DataValue[] values = new DataValue[defs.length];
    
    RecordPointer r = inputRecord.copy();
    for(int i =0; i < defs.length; i++){
      values[i] = defs[i].evaluator.eval();  
    }
    CompoundValue v = new CompoundValue(r, values);
    records.add(v);
  }

  @Override
  protected RecordIterator doWork() {
    StackedComparator r = new StackedComparator(defs);
    QuickSort qs = new QuickSort();
    qs.sort(r, 0, records.size());
    return new OrderIterator();
  }

  
  public class SortDefinition{
    boolean forward;
    boolean nullsLast;
    BasicEvaluator evaluator;
    
    public SortDefinition(BasicEvaluator evaluator, boolean forward) {
      this.evaluator = evaluator;
      this.forward = forward;
    }
  }
  
  private class CompoundValue{
    DataValue[] values;
    RecordPointer record;
    public CompoundValue(RecordPointer record, DataValue[] values) {
      super();
      this.record = record;
      this.values = values;
    }
    
    
  }
  
  private class StackedComparator implements IndexedSortable{
//    private List<DataValue> values;
//    private boolean[] nullsLast;
//    private boolean[] forward;

    public StackedComparator(SortDefinition[] defs){
//      this.nullsLast = new boolean[defs.length];
//      this.forward = new boolean[defs.length];
//      for(int i =0; i < defs.length; i++){
//        nullsLast[i] = defs[i].nullsLast;
//        forward[i] = defs[i].forward;
//      }
    }
    


    @Override
    public void swap(int index0, int index1) {
      CompoundValue v = records.get(index0);
      records.set(index0, records.get(index1));
      records.set(index1, v);
    }

    @Override
    public int compare(int index0, int index1) {
      int result = 0;
      CompoundValue v1 = records.get(index0);
      CompoundValue v2 = records.get(index1);
      
      for(int i =0; i < defs.length; i++){
        boolean nullLast = defs[i].nullsLast;
        boolean asc = defs[i].forward;
        DataValue dv1 = v1.values[i];
        DataValue dv2 = v2.values[i];
        if(dv1 == null){
          if(dv2 == null){
            result = 0;
          }else{
            result = nullLast ? 1 : -1;
          }
        }else if(dv2 == null){
          result = nullLast ? -1 : 1;
        }else{
          if(dv1 instanceof ComparableValue && ((ComparableValue) dv1).supportsCompare(dv2)){
            result = ((ComparableValue)dv1).compareTo(dv2);
            if(!asc) result = -result;
          }else{
            return 0;  // we break even though there may be more evaluators because we should always return the same ordering for non-comparable values no matter the compare order.
          }
        }
        if(result != 0) return result;
      }
      return result;
    }
    
  }
  
  public class OrderIterator implements RecordIterator{
    final Iterator<CompoundValue> iter;
    public OrderIterator() {
      this.iter = records.iterator();
    }
    
    @Override
    public NextOutcome next() {
      if(iter.hasNext()) {
        outputRecord.setRecord(iter.next().record);
        return NextOutcome.INCREMENTED_SCHEMA_CHANGED;
      }
      
      return NextOutcome.NONE_LEFT;
    }

    @Override
    public ROP getParent() {
      return OrderROP.this;
    }

    @Override
    public RecordPointer getRecordPointer() {
      return inputRecord;
    }
  }
}
