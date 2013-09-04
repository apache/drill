package org.apache.drill.exec.ref;

import java.util.Iterator;

import org.apache.drill.exec.ref.rops.ROP;


public interface RecordIterator{
  
  public enum NextOutcome {NONE_LEFT, INCREMENTED_SCHEMA_UNCHANGED, INCREMENTED_SCHEMA_CHANGED}
  public RecordPointer getRecordPointer(); // called once
  public NextOutcome next();
  public ROP getParent();
  
  
  public static class IteratorWrapper implements RecordIterator{
    final Iterator<RecordPointer> iter;
    final RecordPointer outputRecord;
    final ROP parent;
    public IteratorWrapper(ROP rop, Iterator<RecordPointer> iter, RecordPointer outputRecord) {
      this.iter = iter;
      this.parent = rop;
      this.outputRecord = outputRecord;
    }
    
    @Override
    public NextOutcome next() {
      if(iter.hasNext()) {
        outputRecord.copyFrom(iter.next());
        return NextOutcome.INCREMENTED_SCHEMA_CHANGED;
      }
      
      return NextOutcome.NONE_LEFT;
    }

    @Override
    public ROP getParent() {
      return parent;
    }

    @Override
    public RecordPointer getRecordPointer() {
      return outputRecord;
    }

    
    
    
  }
}
