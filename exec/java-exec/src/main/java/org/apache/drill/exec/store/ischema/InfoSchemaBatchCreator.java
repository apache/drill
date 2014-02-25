package org.apache.drill.exec.store.ischema;

import java.util.Collections;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;

public class InfoSchemaBatchCreator implements BatchCreator<InfoSchemaSubScan>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InfoSchemaBatchCreator.class);

  @Override
  public RecordBatch getBatch(FragmentContext context, InfoSchemaSubScan config, List<RecordBatch> children) throws ExecutionSetupException {
    RecordReader rr = new RowRecordReader(context, config.getTable(), context.getRootSchema());
    return new ScanBatch(context, Collections.singleton(rr).iterator());
  }
}
