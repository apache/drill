package org.apache.drill.exec.store.msgpack.valuewriter;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.value.FloatValue;
import org.msgpack.value.Value;

public class FloatValueWriter extends ScalarValueWriter {

  public FloatValueWriter() {
    super();
  }

  @Override
  public void write(Value v, MapWriter mapWriter, String fieldName, ListWriter listWriter, FieldSelection selection,
      MaterializedField schema) {
    FloatValue value = v.asFloatValue();
    MinorType targetSchemaType = getTargetType(MinorType.FLOAT8, schema);
    switch (targetSchemaType) {
    case VARCHAR:
      String s = value.toString();
      writeAsVarChar(s.getBytes(), mapWriter, fieldName, listWriter);
      break;
    case VARBINARY:
      double d = value.toDouble(); // use as double
      ensure(8);
      context.workBuf.setDouble(0, d);
      writeAsVarBinary(mapWriter, fieldName, listWriter, 8);
      break;
    case FLOAT8:
      writeAsFloat8(value.toDouble(), mapWriter, fieldName, listWriter);
      break;
    default:
      throw new DrillRuntimeException("Can't cast " + value.getValueType() + " into " + targetSchemaType);
    }
  }

}
