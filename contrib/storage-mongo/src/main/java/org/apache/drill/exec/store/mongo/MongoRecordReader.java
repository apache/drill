package org.apache.drill.exec.store.mongo;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.RecordReader;

public class MongoRecordReader implements RecordReader {

  public MongoRecordReader(List<SchemaPath> projectedColumns,
      FragmentContext context) {
    
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {

  }

  @Override
  public int next() {
    return 0;
  }

  @Override
  public void cleanup() {

  }

}
