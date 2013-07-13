package org.apache.drill.exec.store.orc;

import org.apache.drill.exec.store.VectorHolder;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

public class DateOrcColumnReader extends OrcColumnReader<LongColumnVector> {

  public DateOrcColumnReader(VectorHolder holder) {
    super(holder);
  }

  @Override
  public void parseNextBatch(long count, LongColumnVector vector) {

  }
}
