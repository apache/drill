package org.apache.drill.exec.store.orc;

import org.apache.drill.exec.store.VectorHolder;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

public class TimestampOrcColumnReader extends OrcColumnReader<LongColumnVector> {
  public TimestampOrcColumnReader(VectorHolder holder) {
    super(holder);
  }

  @Override
  public void parseNextBatch(long count, LongColumnVector vector) {

  }
}
