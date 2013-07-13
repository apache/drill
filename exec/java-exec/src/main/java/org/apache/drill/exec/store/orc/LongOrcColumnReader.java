package org.apache.drill.exec.store.orc;

import org.apache.drill.exec.store.VectorHolder;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

public class LongOrcColumnReader extends OrcColumnReader<LongColumnVector> {

  public LongOrcColumnReader(VectorHolder holder) {
    super(holder);
  }

  @Override
  public void parseNextBatch(long count, LongColumnVector vector) {
    NullableBigIntVector vv = (NullableBigIntVector) holder.getValueVector();
    NullableBigIntVector.Mutator mutator = vv.getMutator();
    long[] values = vector.vector;
    if(vector.noNulls) {
      for(int i = 0; i < count; i++) {
        mutator.set(i, values[i]);
      }
    } else {
      boolean[] isNulls = vector.isNull;
      for(int i = 0; i < count; i++) {
        if(!isNulls[i]) {
          mutator.set(i, values[i]);
        }
      }
    }
  }
}
