package org.apache.drill.exec.physical.impl.join;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.memory.BufferAllocator.PreAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.MergeJoinPOP;
import org.apache.drill.exec.physical.impl.join.JoinWorker.JoinOutcome;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;

/**
 * A merge join combining to incoming in-order batches.
 */
public class MergeJoinBatch extends AbstractRecordBatch<MergeJoinPOP> {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MergeJoinBatch.class);
  
  private final RecordBatch left;
  private final RecordBatch right;
  private final JoinStatus status;
  private JoinWorker worker;
  public MergeJoinBatchBuilder batchBuilder;
  
  protected MergeJoinBatch(MergeJoinPOP popConfig, FragmentContext context, RecordBatch left, RecordBatch right) {
    super(popConfig, context);
    this.left = left;
    this.right = right;
    this.status = new JoinStatus(left, right, this);
    this.batchBuilder = new MergeJoinBatchBuilder(context, status);
  }

  @Override
  public int getRecordCount() {
    return status.outputPosition;
  }

  @Override
  public IterOutcome next() {
    
    // we do this in the here instead of the constructor because don't necessary want to start consuming on construction.
    status.ensureInitial();
    
    // loop so we can start over again if we find a new batch was created.
    while(true){
      
      boolean first = false;
      if(worker == null){
        try {
          this.worker = getNewWorker();
          first = true;
        } catch (ClassTransformationException | IOException e) {
          context.fail(new SchemaChangeException(e));
          kill();
          return IterOutcome.STOP;
        }
      }

      // if the previous outcome was a change in schema or we sent a batch, we have to set up a new batch.
      if(status.getOutcome() == JoinOutcome.BATCH_RETURNED || status.getOutcome() == JoinOutcome.SCHEMA_CHANGED){
        allocateBatch();
      }

      // join until we have a complete outgoing batch
      worker.doJoin(status);

      // get the outcome of the join.
      switch(status.getOutcome()){
      case BATCH_RETURNED:
        // only return new schema if new worker has been setup.
        return first ? IterOutcome.OK_NEW_SCHEMA : IterOutcome.OK;
      case FAILURE:
        kill();
        return IterOutcome.STOP;
      case NO_MORE_DATA:
        return status.outputPosition > 0 ? IterOutcome.OK: IterOutcome.NONE;
      case MODE_CHANGED:
      case SCHEMA_CHANGED:
        worker = null;
        if(status.outputPosition > 0){
          // if we have current data, let's return that.
          return first ? IterOutcome.OK_NEW_SCHEMA : IterOutcome.OK;
        }else{
          // loop again to rebuild worker.
          continue;
        }
      case WAITING:
        return IterOutcome.NOT_YET;
      default:
        throw new IllegalStateException();
      }
    }
  }

  public void resetBatchBuilder() {
    batchBuilder = new MergeJoinBatchBuilder(context, status);
  }

  public void addRightToBatchBuilder() {
    batchBuilder.add(right);
  }

  @Override
  protected void killIncoming() {
    left.kill();
    right.kill();
  }

  private JoinWorker getNewWorker() throws ClassTransformationException, IOException{
    CodeGenerator<JoinWorker> cg = new CodeGenerator<JoinWorker>(JoinWorker.TEMPLATE_DEFINITION, context.getFunctionRegistry());

    // if (status.rightSourceMode)
      // generate copier which deref's SV4
    // else
      // generate direct copier.
    
    // generate comparator.
    // generate compareNextLeftKey.

    JoinWorker w = context.getImplementationClass(cg);
    w.setupJoin(status, this.container);
    return w;
  }

  private void allocateBatch(){
    // allocate new batch space.
  }
}
