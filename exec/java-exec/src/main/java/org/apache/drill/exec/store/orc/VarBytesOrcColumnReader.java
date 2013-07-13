package org.apache.drill.exec.store.orc;

import org.apache.drill.exec.store.VectorHolder;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;

public class VarBytesOrcColumnReader extends OrcColumnReader<BytesColumnVector> {
  public VarBytesOrcColumnReader(VectorHolder holder) {
    super(holder);
  }

  @Override
  public void parseNextBatch(long count, BytesColumnVector vector) {
  }
}
