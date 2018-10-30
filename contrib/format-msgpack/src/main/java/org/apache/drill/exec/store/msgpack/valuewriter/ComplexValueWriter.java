package org.apache.drill.exec.store.msgpack.valuewriter;

import java.util.EnumMap;

import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.msgpack.MsgpackParsingException;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

public abstract class ComplexValueWriter extends AbstractValueWriter {

  protected EnumMap<ValueType, AbstractValueWriter> valueWriterMap;

  public ComplexValueWriter(EnumMap<ValueType, AbstractValueWriter> valueWriterMap) {
    super();
    this.valueWriterMap = valueWriterMap;
  }

  protected void writeElement(Value value, MapWriter mapWriter, ListWriter listWriter, String fieldName,
      FieldSelection selection, MaterializedField schema) {
    try {
      // if (!checkElementSchema(value, schema)) {
      // return MSG_RECORD_PARSE_ERROR;
      // }

      valueWriterMap.get(value.getValueType()).write(value, mapWriter, fieldName, listWriter, selection, schema);
    } catch (Exception e) {
      if (context.lenient) {
        context.warn("Failed to write element name: " + fieldName + " of type: " + value.getValueType()
            + " into list. File: " + context.hadoopPath + " line no: " + context.currentRecordNumberInFile() + " ", e);
      } else {
        throw new MsgpackParsingException(
            "Failed to write element name: " + fieldName + " of type: " + value.getValueType() + " into list. File: "
                + context.hadoopPath + " line no: " + context.currentRecordNumberInFile() + " ",
            e);
      }
    }
  }

//private boolean checkElementSchema(Value value, MaterializedField schema) {
//if (!useSchema) {
//  return true;
//}
//
//ValueType valueType = value.getValueType();
//MinorType schemaType = schema.getType().getMinorType();
//switch (valueType) {
//case INTEGER:
//  return schemaType == MinorType.BIGINT;
//case ARRAY:
//  return schema.getDataMode() == DataMode.REPEATED;
//case BOOLEAN:
//  return schemaType == MinorType.BIT;
//case MAP:
//  return schemaType == MinorType.MAP;
//case FLOAT:
//  return schemaType == MinorType.FLOAT8;
//case EXTENSION:
//  ExtensionValue ev = value.asExtensionValue();
//  byte extType = ev.getType();
//  if (extType == -1) {
//    return schemaType == MinorType.TIMESTAMP;
//  } else {
//    return schemaType == MinorType.VARBINARY;
//  }
//case STRING:
//  return schemaType == MinorType.VARCHAR;
//case BINARY:
//  return schemaType == MinorType.VARBINARY;
//default:
//  throw new DrillRuntimeException("Unsupported msgpack type: " + value);
//}
//}

}
