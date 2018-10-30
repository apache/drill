package org.apache.drill.exec.store.msgpack.valuewriter;

import java.util.ServiceLoader;

import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.Value;

public class ExtensionValueWriter extends ScalarValueWriter {
  private ExtensionValueHandler[] extensionReaders = new ExtensionValueHandler[128];

  public ExtensionValueWriter() {

    ServiceLoader<ExtensionValueHandler> loader = ServiceLoader.load(ExtensionValueHandler.class);
    for (ExtensionValueHandler msgpackExtensionReader : loader) {
      logger.debug("Loaded msgpack extension reader: " + msgpackExtensionReader.getClass());
      msgpackExtensionReader.setup(this.context);
      byte idx = msgpackExtensionReader.getExtensionTypeNumber();
      extensionReaders[idx] = msgpackExtensionReader;
    }
  }

  @Override
  public void write(Value v, MapWriter mapWriter, String fieldName, ListWriter listWriter, FieldSelection selection,
      MaterializedField schema) {

    ExtensionValue ev = v.asExtensionValue();
    byte extType = ev.getType();
    if (extType == -1) {
      extType = 0;
    }

    // Try to find extension type reader for given type.
    ExtensionValueHandler msgpackExtensionReader = extensionReaders[extType];
    if (msgpackExtensionReader != null) {
      msgpackExtensionReader.write(ev, mapWriter, fieldName, listWriter, null, null);
    } else {
      // Fallback to reading extension type as varbinary.
      byte[] bytes = ev.getData();
      writeAsVarBinary(bytes, mapWriter, fieldName, listWriter);
    }
  }
}