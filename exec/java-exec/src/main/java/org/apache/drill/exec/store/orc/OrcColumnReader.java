package org.apache.drill.exec.store.orc;

import org.apache.drill.exec.store.VectorHolder;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;

public abstract class OrcColumnReader<C extends ColumnVector> {
  protected final VectorHolder holder;

  public OrcColumnReader(VectorHolder holder) {
    this.holder = holder;
  }

  public VectorHolder getHolder() {
    return holder;
  }

  public abstract void parseNextBatch(long count, C vector);
}
