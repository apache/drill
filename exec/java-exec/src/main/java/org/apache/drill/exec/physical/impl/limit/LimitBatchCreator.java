package org.apache.drill.exec.physical.impl.limit;

import com.google.common.collect.Iterables;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Limit;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.RecordBatch;

import java.util.List;

public class LimitBatchCreator implements BatchCreator<Limit> {
  @Override
  public RecordBatch getBatch(FragmentContext context, Limit config, List<RecordBatch> children) throws ExecutionSetupException {
    return new LimitRecordBatch(config, context, Iterables.getOnlyElement(children));
  }
}
