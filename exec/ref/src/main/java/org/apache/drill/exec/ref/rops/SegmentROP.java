package org.apache.drill.exec.ref.rops;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.Segment;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.eval.EvaluatorFactory;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.ref.values.DataValueSet;
import org.apache.drill.exec.ref.values.ScalarValues;

import com.google.common.collect.ArrayListMultimap;

public class SegmentROP extends AbstractBlockingOperator<Segment> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SegmentROP.class);
  
  private ArrayListMultimap<DataValueSet, RecordPointer> map;
  private DataValueSet staging;
  
  FieldReference outputSegmentKey;
  
  public SegmentROP(Segment g){
    super(g);
  }

  @Override
  protected void setupEvals(EvaluatorFactory builder) {
    LogicalExpression[] groupings = config.getExprs();
    outputSegmentKey = config.getName();
    map  = ArrayListMultimap.create();
    
    BasicEvaluator[] evals = new BasicEvaluator[groupings.length];
    
    for(int i = 0; i < groupings.length; i++){
      evals[i] = builder.getBasicEvaluator(record, groupings[i]);
    }
    
    staging = new DataValueSet(evals);
    
  }
  
  
  private class BatchIterator implements RecordIterator{
    private DataValueSet previousKeys;
    private final Iterator<Entry<DataValueSet, RecordPointer>> entryIterator;
    private long segmentKey;
    
    public BatchIterator(){
      entryIterator = map.entries().iterator();
    }
    

    @Override
    public RecordPointer getRecordPointer() {
      return outputRecord;
    }

    @Override
    public NextOutcome next() {
      if(!entryIterator.hasNext()) return NextOutcome.NONE_LEFT;
      
      Entry<DataValueSet, RecordPointer> e = entryIterator.next();
      RecordPointer r = e.getValue();
      if(r == null) throw new UnsupportedOperationException();
      outputRecord.setRecord(r);

      if(!e.getKey().equals(previousKeys)){
        previousKeys = e.getKey();
        segmentKey++;
      }
      
      r.addField(outputSegmentKey, new ScalarValues.LongScalar(segmentKey));
      return NextOutcome.INCREMENTED_SCHEMA_CHANGED;
    }

    @Override
    public ROP getParent() {
      return SegmentROP.this;
    }
  }
  
  
  @Override
  protected void consumeRecord() {
    staging.grabValues();
    map.put(staging.cloneValuesOnly(), record.copy());
  }

  
  @Override
  protected RecordIterator doWork() {
    return new BatchIterator();
  }


  
}
