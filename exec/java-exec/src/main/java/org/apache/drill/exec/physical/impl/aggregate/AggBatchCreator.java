package org.apache.drill.exec.physical.impl.aggregate;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.StreamingAggregate;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.RecordBatch;

import com.google.common.base.Preconditions;

public class AggBatchCreator implements BatchCreator<StreamingAggregate>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AggBatchCreator.class);

  @Override
  public RecordBatch getBatch(FragmentContext context, StreamingAggregate config, List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.size() == 1);
    return new AggBatch(config, children.iterator().next(), context);
  }
  
  
}
