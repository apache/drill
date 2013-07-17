package org.apache.drill.exec.physical.impl.project;

import java.util.List;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

import com.google.common.collect.ImmutableList;

public abstract class ProjectorTemplate implements Projector {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProjectorTemplate.class);
  
  private ImmutableList<TransferPair> transfers;
  private SelectionVector2 vector2;
  private SelectionVector4 vector4;
  private SelectionVectorMode svMode;
  
  public ProjectorTemplate() throws SchemaChangeException{
  }

  @Override
  public final int projectRecords(final int recordCount, int firstOutputIndex) {
    switch(svMode){
    case FOUR_BYTE:
      throw new UnsupportedOperationException();
      
      
    case TWO_BYTE:
      final int count = recordCount*2;
      for(int i = 0; i < count; i+=2, firstOutputIndex++){
        doEval(vector2.getIndex(i), firstOutputIndex);
      }
      return recordCount;
      
      
    case NONE:
      
      for(TransferPair t : transfers){
        t.transfer();
      }
      final int countN = recordCount;
      for (int i = 0; i < countN; i++, firstOutputIndex++) {
        doEval(i, firstOutputIndex);
      }
      return recordCount;
      
      
    default:
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public final void setup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing, List<TransferPair> transfers)  throws SchemaChangeException{

    this.svMode = incoming.getSchema().getSelectionVector(); 
    switch(svMode){
    case FOUR_BYTE:
      this.vector4 = incoming.getSelectionVector4();
      break;
    case TWO_BYTE:
      this.vector2 = incoming.getSelectionVector2();
      break;
    }
    this.transfers = ImmutableList.copyOf(transfers);
    setupEval(context, incoming, outgoing);
  }

  protected abstract void setupEval(FragmentContext context, RecordBatch incoming, RecordBatch outgoing) throws SchemaChangeException;
  protected abstract void doEval(int inIndex, int outIndex);

  


}
