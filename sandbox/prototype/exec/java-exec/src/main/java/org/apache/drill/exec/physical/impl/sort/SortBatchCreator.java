package org.apache.drill.exec.physical.impl.sort;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Sort;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.RecordBatch;

import com.google.common.base.Preconditions;

public class SortBatchCreator implements BatchCreator<Sort>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SortBatchCreator.class);

  @Override
  public RecordBatch getBatch(FragmentContext context, Sort config, List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.size() == 1);
    return new SortBatch(config, context, children.iterator().next());
  }
  
  
}
