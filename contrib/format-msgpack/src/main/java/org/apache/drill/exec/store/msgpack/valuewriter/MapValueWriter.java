package org.apache.drill.exec.store.msgpack.valuewriter;

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.msgpack.MsgpackParsingException;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

public class MapValueWriter extends ComplexValueWriter {

  public MapValueWriter(EnumMap<ValueType, AbstractValueWriter> valueWriterMap) {
    super(valueWriterMap);
  }

  @Override
  public void write(Value v, MapWriter mapWriter, String fieldName, ListWriter listWriter, FieldSelection selection,
      MaterializedField schema) {

//    if (context.useSchema) {
//      if (schema == null) {
//        context.warn("Writing a MAP value but target schema is null. FieldName: " + fieldName + " target type: "
//            + schema.getType() + " path is: " + printPath(mapWriter, listWriter));
//        return;
//      } else if (schema.getType().getMinorType() != MinorType.MAP) {
//        context.warn("Writing a MAP value but target schema type is: " + schema.getType() + " path is: "
//            + printPath(mapWriter, listWriter));
//        return;
//      }
//    }

    MapValue value = v.asMapValue();
    MapWriter subMapWriter;
    if (mapWriter != null) {
      // Write map in a map.
      subMapWriter = mapWriter.map(fieldName);
    } else {
      // Write map in a list.
      subMapWriter = listWriter.map();
    }
    writeToMap(value, subMapWriter, selection, schema);
  }

  public void writeToMap(MapValue value, MapWriter writer, FieldSelection selection, MaterializedField schema) {

    writer.start();
    try {
      Set<Map.Entry<Value, Value>> valueEntries = value.entrySet();
      for (Map.Entry<Value, Value> valueEntry : valueEntries) {
        Value key = valueEntry.getKey();
        Value element = valueEntry.getValue();
        if (element.isNilValue()) {
          continue;
        }
        String fieldName = getFieldName(key);
        if (fieldName == null) {
          if (context.lenient) {
            context.parseWarn();
            continue;
          } else {
            throw new MsgpackParsingException("Failed to parse fieldname.");
          }
        }
        FieldSelection childSelection = selection.getChild(fieldName);
        if (childSelection.isNeverValid()) {
          continue;
        }
        MaterializedField childSchema = getChildSchema(schema, fieldName);
        writeElement(element, writer, null, fieldName, childSelection, childSchema);
      }
    } finally {
      writer.end();
    }
  }

  private MaterializedField getChildSchema(MaterializedField schema, String fieldName) {
    if (!context.useSchema) {
      return null;
    }
    for (MaterializedField c : schema.getChildren()) {
      if (fieldName.equalsIgnoreCase(c.getName())) {
        return c;
      }
    }
    throw new MsgpackParsingException("Field name: " + fieldName + " has no child schema.");
  }

  private String getFieldName(Value v) {

    String fieldName = null;

    ValueType valueType = v.getValueType();
    switch (valueType) {
    case STRING:
      fieldName = v.asStringValue().asString();
      break;
    case BINARY:
      byte[] bytes = v.asBinaryValue().asByteArray();
      fieldName = new String(bytes);
      break;
    case INTEGER:
      IntegerValue iv = v.asIntegerValue();
      fieldName = iv.toString();
      break;
    case ARRAY:
    case BOOLEAN:
    case MAP:
    case FLOAT:
    case EXTENSION:
    case NIL:
      break;
    default:
      throw new DrillRuntimeException("UnSupported msgpack type: " + valueType);
    }
    return fieldName;
  }
}
