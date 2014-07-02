package org.apache.drill.exec.store.mongo;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.RecordBatch;

public class MongoScanBatchCreator implements BatchCreator<MongoSubScan>{

  @Override
  public RecordBatch getBatch(FragmentContext context, MongoSubScan config,
      List<RecordBatch> children) throws ExecutionSetupException {
    return null;
  }

}
