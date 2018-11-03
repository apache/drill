package org.apache.drill.exec.store.msgpack.valuewriter;

import java.util.Collection;
import java.util.EnumMap;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.msgpack.MsgpackParsingException;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

public class ArrayValueWriter extends ComplexValueWriter {

  private List<ListWriter> emptyArrayWriters;

  public ArrayValueWriter(EnumMap<ValueType, AbstractValueWriter> valueWriterMap, List<ListWriter> emptyArrayWriters) {
    super(valueWriterMap);
    this.emptyArrayWriters = emptyArrayWriters;
  }

  @Override
  public void write(Value v, MapWriter mapWriter, String fieldName, ListWriter listWriter, FieldSelection selection,
      MaterializedField schema) {

//    if (context.useSchema) {
//      if (schema == null) {
//        context.warn("Writing a ARRAY value but target schema is null. FieldName: " + fieldName + " target type: "
//            + schema.getType() + " path is: " + printPath(mapWriter, listWriter));
//        return;
//      } else if (schema.getDataMode() != DataMode.REPEATED) {
//        context.warn("Writing a ARRAY value but target schema type is not repeated it is minortype: "
//            + schema.getType().getMinorType() + " path is: " + printPath(mapWriter, listWriter));
//        return;
//      } else if (schema.getType().getMinorType() != MinorType.LIST) {
//        context.warn("Writing a ARRAY value but target schema type is not LIST, minortype: "
//            + schema.getType().getMinorType() + " path is: " + printPath(mapWriter, listWriter));
//        return;
//      }
//    }

    ArrayValue value = v.asArrayValue();
    ListWriter subListWriter;
    MaterializedField childSchema;
    if (mapWriter != null) {
      // Write array in map.
      subListWriter = mapWriter.list(fieldName);
      childSchema = getArrayInMapChildSchema(fieldName, schema);
    } else {
      // Write array in array.
      subListWriter = listWriter.list();
      childSchema = getArrayInArrayChildSchema(schema);
    }
    writeToList(value, subListWriter, selection, childSchema);
  }

  private MaterializedField getArrayInMapChildSchema(String fieldName, MaterializedField schema) {
    if (!context.useSchema) {
      return null;
    }
    if (schema.getType().getMinorType() == MinorType.MAP) {
      return schema;
    } else {
      Collection<MaterializedField> children = schema.getChildren();
      MaterializedField childSchema = children.iterator().next();
      if (childSchema == null) {
        throw new MsgpackParsingException("Field name: " + fieldName + " has no child schema.");
      }
      return childSchema;
    }
  }

  private MaterializedField getArrayInArrayChildSchema(MaterializedField schema) {
    if (!context.useSchema) {
      return null;
    }
    Collection<MaterializedField> children = schema.getChildren();
    MaterializedField childSchema = children.iterator().next();
    if (childSchema == null) {
      throw new MsgpackParsingException("Array in array element has no child schema.");
    }
    return childSchema;
  }

  private void writeToList(Value value, ListWriter listWriter, FieldSelection selection, MaterializedField schema) {
    listWriter.startList();
    try {
      ArrayValue arrayValue = value.asArrayValue();
      for (int i = 0; i < arrayValue.size(); i++) {
        Value element = arrayValue.get(i);
        if (!element.isNilValue()) {
          writeElement(element, null, listWriter, null, selection, schema);
        }
      }
    } finally {
      addIfNotInitialized(listWriter);
      listWriter.endList();
    }
  }

  /**
   * Checks that list has not been initialized and adds it to the
   * emptyArrayWriters collection.
   *
   * @param list ListWriter that should be checked
   */
  private void addIfNotInitialized(ListWriter list) {
    if (list.getValueCapacity() == 0) {
      emptyArrayWriters.add(list);
    }
  }

}
