package org.apache.drill.exec.store.orc;

import org.apache.drill.exec.store.VectorHolder;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;

public class FloatOrcColumnReader extends OrcColumnReader<DoubleColumnVector> {
  public FloatOrcColumnReader(VectorHolder holder) {
    super(holder);
  }

  @Override
  public void parseNextBatch(long count, DoubleColumnVector vector) {
    NullableFloat4Vector vv = (NullableFloat4Vector) holder.getValueVector();
    NullableFloat4Vector.Mutator mutator = vv.getMutator();
    double[] values = vector.vector;
    if(vector.noNulls) {
      for(int i = 0; i < count; i++) {
        mutator.set(i, (float) values[i]);
      }
    } else {
      boolean[] isNulls = vector.isNull;
      for(int i = 0; i < count; i++) {
        if(!isNulls[i]) {
          mutator.set(i, (float) values[i]);
        }
      }
    }
  }
}
