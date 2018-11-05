/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.msgpack.valuewriter;

import java.util.ServiceLoader;

import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.msgpack.MsgpackReaderContext;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.Value;

public class ExtensionValueWriter extends ScalarValueWriter {
  private ExtensionValueHandler[] extensionReaders = new ExtensionValueHandler[128];

  public ExtensionValueWriter() {
  }

  @Override
  public void setup(MsgpackReaderContext context) {
    super.setup(context);
    ServiceLoader<ExtensionValueHandler> loader = ServiceLoader.load(ExtensionValueHandler.class);
    for (ExtensionValueHandler msgpackExtensionReader : loader) {
      logger.debug("Loaded msgpack extension reader: " + msgpackExtensionReader.getClass());
      msgpackExtensionReader.setup(context);
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