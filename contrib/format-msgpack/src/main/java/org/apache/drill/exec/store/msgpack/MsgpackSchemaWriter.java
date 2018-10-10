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
package org.apache.drill.exec.store.msgpack;

import java.io.IOException;
import java.util.Collection;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;

public class MsgpackSchemaWriter {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MsgpackSchemaWriter.class);

  public MsgpackSchemaWriter() {
  }

  public void applySchema(MaterializedField schema, ComplexWriter writer) throws IOException {
    writeToMap(schema, writer.rootAsMap());
  }

  private void writeToList(MaterializedField schema, ListWriter listWriter) throws IOException {
    if (schema.getType().getMinorType() == MinorType.MAP) {
      writeToMap(schema, listWriter.map());
    } else {
      Collection<MaterializedField> children = schema.getChildren();
      MaterializedField element = children.iterator().next();
      writeElement(element, null, listWriter, null);
    }
  }

  private void writeToMap(MaterializedField schema, MapWriter writer) throws IOException {
    for (MaterializedField element : schema.getChildren()) {
      String name = element.getName();
      writeElement(element, writer, null, name);
    }
  }

  private void writeElement(MaterializedField schema, MapWriter mapWriter, ListWriter listWriter, String name)
      throws IOException {
    if (schema.getDataMode() == DataMode.REPEATED) {
      writeRepeatedElement(schema, mapWriter, listWriter, name);
    } else {
      writeSingleElement(schema, mapWriter, listWriter, name);
    }
  }

  private void writeRepeatedElement(MaterializedField schema, MapWriter mapWriter, ListWriter listWriter, String name)
      throws IOException {
    if (mapWriter != null) {
      writeToList(schema, mapWriter.list(name));
    } else {
      writeToList(schema, listWriter.list());
    }
  }

  private void writeSingleElement(MaterializedField schema, MapWriter mapWriter, ListWriter listWriter, String name)
      throws IOException {
    MinorType valueType = schema.getType().getMinorType();
    switch (valueType) {
    case BIGINT:
      writeInt64(mapWriter, name, listWriter);
      break;
    case LIST:
      if (mapWriter != null) {
        writeToList(schema, mapWriter.list(name));
      } else {
        writeToList(schema, listWriter.list());
      }
      break;
    case BIT:
      writeBoolean(mapWriter, name, listWriter);
      break;
    case MAP:
      // To handle nested Documents.
      if (mapWriter != null) {
        writeToMap(schema, mapWriter.map(name));
      } else {
        writeToMap(schema, listWriter.map());
      }
      break;
    case FLOAT8:
      writeDouble(mapWriter, name, listWriter);
      break;
    case TIMESTAMP:
      writeTimestamp(mapWriter, name, listWriter);
      break;
    case VARCHAR:
      writeString(mapWriter, name, listWriter);
      break;
    case VARBINARY:
      writeBinary(mapWriter, name, listWriter);
      break;
    default:
      logger.warn("UnSupported messagepack type: " + valueType);
    }
  }

  private void writeTimestamp(MapWriter mapWriter, String name, ListWriter listWriter) {
    if (mapWriter != null) {
      mapWriter.timeStamp(name);
    } else {
      listWriter.timeStamp();
    }
  }

  private void writeBinary(MapWriter mapWriter, String name, ListWriter listWriter) {
    if (mapWriter != null) {
      mapWriter.varBinary(name);
    } else {
      listWriter.varBinary();
    }
  }

  private void writeString(MapWriter mapWriter, String name, ListWriter listWriter) {
    if (mapWriter != null) {
      mapWriter.varChar(name);
    } else {
      listWriter.varChar();
    }
  }

  private void writeDouble(MapWriter mapWriter, String name, ListWriter listWriter) {
    if (mapWriter != null) {
      mapWriter.float8(name);
    } else {
      listWriter.float8();
    }
  }

  private void writeBoolean(MapWriter mapWriter, String name, ListWriter listWriter) {
    if (mapWriter != null) {
      mapWriter.bit(name);
    } else {
      listWriter.bit();
    }
  }

  private void writeInt64(MapWriter mapWriter, String name, ListWriter listWriter) {
    if (mapWriter != null) {
      mapWriter.bigInt(name);
    } else {
      listWriter.bigInt();
    }
  }

}