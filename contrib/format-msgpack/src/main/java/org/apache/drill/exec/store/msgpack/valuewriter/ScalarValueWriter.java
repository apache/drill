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

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.writer.RepeatedVarBinaryWriter;
import org.apache.drill.exec.vector.complex.writer.RepeatedVarCharWriter;
import org.apache.drill.exec.vector.complex.writer.VarBinaryWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.value.Value;

public abstract class ScalarValueWriter extends AbstractValueWriter {

  public ScalarValueWriter() {
  }

  protected void log(Value value, MapWriter mapWriter, String fieldName, ListWriter listWriter,
      FieldSelection selection, MinorType targetSchemaType) {
      logger.debug("      type: '{}' ---> coerced to: '{}'",
          value.getValueType(), targetSchemaType);
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
      VarBinaryWriter w = listWriter.varBinary();
      if(w instanceof RepeatedVarBinaryWriter){
        System.out.println("RepeatedVarBinaryWriter");
      }
      if(w instanceof RepeatedVarCharWriter){
        System.out.println("RepeatedVarCharWriter");
      }
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
