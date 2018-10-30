package org.apache.drill.exec.store.msgpack.valuewriter;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;

public abstract class ScalarValueWriter extends AbstractValueWriter {

  public ScalarValueWriter() {
  }

  protected MinorType getTargetType(MinorType defaultType, MaterializedField schema) {
    if (context.useSchema) {
      return schema.getType().getMinorType();
    }
    return defaultType;
  }

  protected void writeAsVarBinary(byte[] bytes, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    int length = bytes.length;
    ensure(length);
    context.workBuf.setBytes(0, bytes);
    writeAsVarBinary(mapWriter, fieldName, listWriter, length);
  }

  protected void writeAsVarBinary(MapWriter mapWriter, String fieldName, ListWriter listWriter, int length) {
    if (mapWriter != null) {
      mapWriter.varBinary(fieldName).writeVarBinary(0, length, context.workBuf);
    } else {
      listWriter.varBinary().writeVarBinary(0, length, context.workBuf);
    }
  }

  protected void writeAsVarChar(byte[] readString, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    int length = readString.length;
    ensure(length);
    context.workBuf.setBytes(0, readString);
    writeAsVarChar(mapWriter, fieldName, listWriter, length);
  }

  protected void writeAsVarChar(MapWriter mapWriter, String fieldName, ListWriter listWriter, int length) {
    if (mapWriter != null) {
      mapWriter.varChar(fieldName).writeVarChar(0, length, context.workBuf);
    } else {
      listWriter.varChar().writeVarChar(0, length, context.workBuf);
    }
  }

  protected void writeAsFloat8(double d, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    if (mapWriter != null) {
      mapWriter.float8(fieldName).writeFloat8(d);
    } else {
      listWriter.float8().writeFloat8(d);
    }
  }

  protected void writeAsBit(boolean readBoolean, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    int value = readBoolean ? 1 : 0;
    if (mapWriter != null) {
      mapWriter.bit(fieldName).writeBit(value);
    } else {
      listWriter.bit().writeBit(value);
    }
  }

  protected void writeAsBigInt(long value, MapWriter mapWriter, String fieldName, ListWriter listWriter) {
    if (mapWriter != null) {
      mapWriter.bigInt(fieldName).writeBigInt(value);
    } else {
      listWriter.bigInt().writeBigInt(value);
    }
  }

  protected void ensure(final int length) {
    context.workBuf = context.workBuf.reallocIfNeeded(length);
  }

}
