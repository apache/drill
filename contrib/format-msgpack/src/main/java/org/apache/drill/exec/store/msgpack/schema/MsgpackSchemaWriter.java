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
package org.apache.drill.exec.store.msgpack.schema;

import java.io.IOException;
import java.util.Collection;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.msgpack.FieldPathTracker;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;

/**
 * This class uses the desired schema to fill in any missing map fields which
 * were not encountered in a batch. So for example if a field in a map should be
 * of type bigint and we have not seen it in the batch this class will use the
 * MapWriter calling the bigInt(name) method. However it will not put any value
 * into it. However this is usefull because in the next batch we might encounter
 * these values and thus the schema we return between batches will not change.
 * <code>
 *   if (mapWriter != null) {
 *     mapWriter.bigInt(name);
 *   } else {
 *    listWriter.bigInt();
 *  }
 * </code> Note we do the same for lists. We create the list with the proper
 * type. <br>
 * Also not this class does not try to determine if field is really missing.
 * Calling bigInt(name) when the field already exists has no effect on the
 * MapWriter. It already guards agains that.
 */
public class MsgpackSchemaWriter {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MsgpackSchemaWriter.class);

  /**
   * Keep track of the navigation inside the msgpack message. We use this to help
   * track down issues with the data files.
   */
  private final FieldPathTracker fieldPathTracker = new FieldPathTracker();

  public MsgpackSchemaWriter() {
  }

  /**
   * Apply the give schema to the writer. This will fill in any missing field or
   * array.
   *
   * @param schema
   * @param writer
   * @throws IOException
   */
  public void applySchema(MaterializedField schema, ComplexWriter writer) throws IOException {
    fieldPathTracker.reset();
    writeToMap(schema, writer.rootAsMap());
  }

  /**
   * Fill in missing fields in an array.
   */
  private void writeToList(MaterializedField schema, ListWriter listWriter) throws IOException {
    fieldPathTracker.enter("[]");
    if (schema.getType().getMinorType() == MinorType.MAP) {
      // If the array contains elements of type MAP. We create map writer inside the
      // list, and ask to fill in the missing fields in the map.
      writeToMap(schema, listWriter.map());
    } else {
      // We are not dealing with a repated map. It could be a repeated bigint,
      // repeated varchar, repeated list etc.
      // We get the first child of the list schema which will give us the type of the
      // elements either bigint, varchar, list etc.
      Collection<MaterializedField> children = schema.getChildren();
      MaterializedField element = children.iterator().next();
      writeElement(element, null, listWriter, null);
    }
    fieldPathTracker.leave();
  }

  /**
   * Fill in missing fields in a map.
   */
  private void writeToMap(MaterializedField schema, MapWriter writer) throws IOException {
    for (MaterializedField schemaOfField : schema.getChildren()) {
      String name = schemaOfField.getName();
      fieldPathTracker.enter(name);
      writeElement(schemaOfField, writer, null, name);
      fieldPathTracker.leave();
    }
  }

  /**
   * This method is called in two circumstance. Either because we are inside a map
   * or inside a list. In the case of a map we recieve the schema of the field. If
   * in a list we recieve the type of the elements.
   *
   * @param schema
   * @param mapWriter
   * @param listWriter
   * @param name
   * @throws IOException
   */
  private void writeElement(MaterializedField schema, MapWriter mapWriter, ListWriter listWriter, String name)
      throws IOException {

    logger.debug("filling field path: '{}' with type: '{}' mode: '{}'", fieldPathTracker,
        schema.getType().getMinorType(), schema.getDataMode());

    if (schema.getDataMode() == DataMode.REPEATED) {
      // If the schema type is repeated we want to create a sub list.
      writeRepeatedElement(schema, mapWriter, listWriter, name);
    } else {
      writeSingleElement(schema, mapWriter, listWriter, name);
    }
  }

  /**
   * When the element is of type REPEATED we create a sub list either inside a map
   * or inside a list.
   *
   * @param schema
   * @param mapWriter
   * @param listWriter
   * @param name
   * @throws IOException
   */
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
    case LIST:
      if (mapWriter != null) {
        // We are writing a map and the schema says the sub field should be of type
        // LIST.
        writeToList(schema, mapWriter.list(name));
      } else {
        // We are writing an array and the schema says elements should be of type LIST.
        writeToList(schema, listWriter.list());
      }
      break;
    case MAP:
      if (mapWriter != null) {
        // We are writing a map and the schema says the sub field should be of type MAP.
        writeToMap(schema, mapWriter.map(name));
      } else {
        // We are writing an array and the schema says elements should be of type MAP.
        writeToMap(schema, listWriter.map());
      }
      break;
    case BIT:
      // If writing a map the schema says the field should be of type BIT.
      // If writing an array the schema says the elements should be of type BIT.
      writeBoolean(mapWriter, name, listWriter);
      break;
    case BIGINT:
      writeInt64(mapWriter, name, listWriter);
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