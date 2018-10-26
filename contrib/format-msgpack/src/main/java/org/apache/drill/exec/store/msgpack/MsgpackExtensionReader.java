package org.apache.drill.exec.store.msgpack;

import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.value.ExtensionValue;

public interface MsgpackExtensionReader {

  boolean handlesType(byte extType);

  void write(ExtensionValue ev, MapWriter mapWriter, String fieldName, ListWriter listWriter);

}
