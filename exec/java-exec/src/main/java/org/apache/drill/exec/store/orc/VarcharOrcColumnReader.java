package org.apache.drill.exec.store.orc;

import io.netty.buffer.ByteBuf;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.store.VectorHolder;
import org.apache.drill.exec.vector.NullableTinyIntVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

import java.nio.ByteBuffer;

public class VarcharOrcColumnReader extends OrcColumnReader<BytesColumnVector>  {
  public VarcharOrcColumnReader(VectorHolder holder) {
    super(holder);
  }

  @Override
  public void parseNextBatch(long count, BytesColumnVector vector) {
    NullableVarCharVector vv = (NullableVarCharVector) holder.getValueVector();
    NullableVarCharVector.Mutator mutator = vv.getMutator();
    byte[][] values = vector.vector;
    int[] starts = vector.start;
    int[] lengths = vector.length;
    if(vector.noNulls) {
      for(int i = 0; i < count; i++) {
        mutator.set(i, starts[i], lengths[i], values[i]);
      }
    } else {
      boolean[] isNulls = vector.isNull;
      for(int i = 0; i < count; i++) {
        if(!isNulls[i]) {
          mutator.set(i, starts[i], lengths[i], values[i]);
        } else {
          mutator.markNull(i);
        }
      }
    }
  }
}
