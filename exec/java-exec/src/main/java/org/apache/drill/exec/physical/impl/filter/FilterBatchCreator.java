package org.apache.drill.exec.physical.impl.filter;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Filter;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.RecordBatch;

import com.google.common.base.Preconditions;

public class FilterBatchCreator implements BatchCreator<Filter>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FilterBatchCreator.class);

  @Override
  public RecordBatch getBatch(FragmentContext context, Filter config, List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.size() == 1);
    return new FilterRecordBatch(config, children.iterator().next(), context);
  }
  
  
}
