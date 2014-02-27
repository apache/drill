package org.apache.drill.exec.store.orc;

import org.apache.drill.exec.store.VectorHolder;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;

public class DoubleOrcColumnReader extends OrcColumnReader<DoubleColumnVector> {
  public DoubleOrcColumnReader(VectorHolder holder) {
    super(holder);
  }

  @Override
  public void parseNextBatch(long count, DoubleColumnVector vector) {
    NullableFloat8Vector vv = (NullableFloat8Vector) holder.getValueVector();
    NullableFloat8Vector.Mutator mutator = vv.getMutator();
    double[] values = vector.vector;
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
