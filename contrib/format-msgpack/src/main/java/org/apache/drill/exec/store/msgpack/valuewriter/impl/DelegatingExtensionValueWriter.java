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
package org.apache.drill.exec.store.msgpack.valuewriter.impl;

import java.util.ServiceLoader;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.store.msgpack.MsgpackReaderContext;
import org.apache.drill.exec.store.msgpack.valuewriter.ExtensionValueWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

import io.netty.buffer.DrillBuf;

/**
 * This class hanles the msgpack EXTENSION types. The ext format provides a
 * means of providing third party data encodings. For example msgpack defines an
 * extended type for timestamps. <a href=
 * "https://github.com/msgpack/msgpack/blob/master/spec.md#timestamp-extension-type">
 * timestamp extension type</a> <br>
 * This class discovers any {@link ExtensionValueWriter} contributed via Java
 * Plugins, loads the class and dispatches writing to it when it sees the
 * corresponding type code. See {@link TimestampValueWriter} for an example of a
 * {@link ExtensionValueWriter}
 */
public class DelegatingExtensionValueWriter extends AbstractScalarValueWriter {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(DelegatingExtensionValueWriter.class);
  private ExtensionValueWriter[] extensionReaders = new ExtensionValueWriter[128];

  public DelegatingExtensionValueWriter() {
  }

  @Override
  public ValueType getMsgpackValueType() {
    return ValueType.EXTENSION;
  }

  @Override
  public void setup(MsgpackReaderContext context, DrillBuf drillBuf) {
    super.setup(context, drillBuf);
    ServiceLoader<ExtensionValueWriter> loader = ServiceLoader.load(ExtensionValueWriter.class);
    for (ExtensionValueWriter msgpackExtensionReader : loader) {
      logger.debug("Loaded msgpack extension reader: " + msgpackExtensionReader.getClass());
      msgpackExtensionReader.setup(context, drillBuf);
      byte idx = msgpackExtensionReader.getExtensionTypeNumber();
      extensionReaders[idx] = msgpackExtensionReader;
    }
  }

  @Override
  public MinorType getDefaultType(Value v) {
    ExtensionValue ev = v.asExtensionValue();
    byte extType = ev.getType();
    if (extType == -1) {
      extType = 0;
    }

    // Try to find extension type reader for given type.
    ExtensionValueWriter msgpackExtensionReader = extensionReaders[extType];
    if (msgpackExtensionReader != null) {
      return msgpackExtensionReader.getDefaultType(v);
    }

    return MinorType.VARBINARY;
  }

  @Override
  public void doWrite(Value v, MapWriter mapWriter, String fieldName, ListWriter listWriter,
      MinorType targetSchemaType) {

    ExtensionValue ev = v.asExtensionValue();
    byte extType = ev.getType();
    if (extType == -1) {
      extType = 0;
    }

    // Try to find extension type reader for given type.
    ExtensionValueWriter msgpackExtensionReader = extensionReaders[extType];
    if (msgpackExtensionReader != null) {
      msgpackExtensionReader.doWrite(ev, mapWriter, fieldName, listWriter, targetSchemaType);
    } else {
      // Fallback to reading extension type as varbinary.
      writeAsVarBinary(ev.getData(), mapWriter, fieldName, listWriter);
    }
  }
}