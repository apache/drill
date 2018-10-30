package org.apache.drill.exec.store.msgpack.valuewriter;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.value.BooleanValue;
import org.msgpack.value.Value;

public class BooleanValueWriter extends ScalarValueWriter {

  public BooleanValueWriter() {
  }

  @Override
  public void write(Value v, MapWriter mapWriter, String fieldName, ListWriter listWriter, FieldSelection selection,
      MaterializedField schema) {
    BooleanValue value = v.asBooleanValue();
    MinorType targetSchemaType = getTargetType(MinorType.BIT, schema);
    switch (targetSchemaType) {
    case VARCHAR:
      String s = value.toString();
      writeAsVarChar(s.getBytes(), mapWriter, fieldName, listWriter);
      break;
    case VARBINARY:
      boolean b = value.getBoolean();
      ensure(1);
      context.workBuf.setBoolean(0, b);
      writeAsVarBinary(mapWriter, fieldName, listWriter, 1);
      break;
    case BIT:
      writeAsBit(value.getBoolean(), mapWriter, fieldName, listWriter);
      break;
    default:
      throw new DrillRuntimeException("Can't cast " + value.getValueType() + " into " + targetSchemaType);
    }
  }

}
