package org.apache.drill.exec.store.msgpack.valuewriter;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;

public class StringValueWriter extends ScalarValueWriter {

  public StringValueWriter() {
  }

  @Override
  public void write(Value v, MapWriter mapWriter, String fieldName, ListWriter listWriter, FieldSelection selection,
      MaterializedField schema) {
    StringValue value = v.asStringValue();
    MinorType targetSchemaType = getTargetType(MinorType.VARCHAR, schema);
    switch (targetSchemaType) {
    case VARCHAR:
      byte[] buff = value.asByteArray();
      writeAsVarChar(buff, mapWriter, fieldName, listWriter);
      break;
    case VARBINARY:
      byte[] binBuff = value.asByteArray();
      writeAsVarBinary(binBuff, mapWriter, fieldName, listWriter);
      break;
    default:
      throw new DrillRuntimeException("Can't cast " + value.getValueType() + " into " + targetSchemaType);
    }
  }

}
