package org.apache.drill.exec.store.msgpack.valuewriter;

import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.msgpack.MsgpackReaderContext;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.value.Value;

public interface ValueWriter {

  public void setup(MsgpackReaderContext context);

  public void write(Value value, MapWriter mapWriter, String fieldName, ListWriter listWriter, FieldSelection selection,
      MaterializedField schema);

}
