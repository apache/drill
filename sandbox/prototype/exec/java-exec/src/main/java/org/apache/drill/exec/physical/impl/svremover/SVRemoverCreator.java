package org.apache.drill.exec.physical.impl.svremover;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.SelectionVectorRemover;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.RecordBatch;

import com.google.common.base.Preconditions;

public class SVRemoverCreator implements BatchCreator<SelectionVectorRemover>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SVRemoverCreator.class);

  @Override
  public RecordBatch getBatch(FragmentContext context, SelectionVectorRemover config, List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.size() == 1);
    return new RemovingRecordBatch(children.iterator().next(), context);
  }
  
  
}
