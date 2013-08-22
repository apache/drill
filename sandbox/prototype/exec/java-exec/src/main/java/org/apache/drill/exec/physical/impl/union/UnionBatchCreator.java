package org.apache.drill.exec.physical.impl.union;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Union;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.RecordBatch;

import com.google.common.base.Preconditions;

public class UnionBatchCreator implements BatchCreator<Union>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnionBatchCreator.class);

  @Override
  public RecordBatch getBatch(FragmentContext context, Union config, List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.size() >= 1);
    return new UnionRecordBatch(config, children, context);
  }
  
  
}
