package org.apache.drill.exec.record;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;

public abstract class AbstractSingleRecordBatch<T extends PhysicalOperator> extends AbstractRecordBatch<T> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractSingleRecordBatch.class);
  
  protected final RecordBatch incoming;
  
  public AbstractSingleRecordBatch(T popConfig, FragmentContext context, RecordBatch incoming) {
    super(popConfig, context);
    this.incoming = incoming;
  }

  @Override
  protected void killIncoming() {
    incoming.kill();
  }

  @Override
  public IterOutcome next() {
    IterOutcome upstream = incoming.next();
    
    switch(upstream){
    case NONE:
    case NOT_YET:
    case STOP:
      container.clear();
      return upstream;
    case OK_NEW_SCHEMA:
      try{
        setupNewSchema();
      }catch(SchemaChangeException ex){
        kill();
        logger.error("Failure during query", ex);
        context.fail(ex);
        return IterOutcome.STOP;
      }
      // fall through.
    case OK:
      doWork();
      return upstream; // change if upstream changed, otherwise normal.
    default:
      throw new UnsupportedOperationException();
    }
  }
  
  protected abstract void setupNewSchema() throws SchemaChangeException;
  protected abstract void doWork();
}
