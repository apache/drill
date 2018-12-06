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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ValueType;

public class FloatValueWriter extends AbstractScalarValueWriter {

  private final ByteBuffer floatBuffer = ByteBuffer.allocate(32);

  public FloatValueWriter() {
    super();
  }

  @Override
  public MinorType getDefaultType() {
    return MinorType.FLOAT8;
  }

  @Override
  public ValueType getMsgpackValueType() {
    return ValueType.FLOAT;
  }

  @Override
  public void doWrite(MessageUnpacker unpacker, MapWriter mapWriter, String fieldName, ListWriter listWriter,
      MinorType targetSchemaType) throws IOException {
    double unpackDouble = unpacker.unpackDouble();
    switch (targetSchemaType) {
    case VARCHAR: {
      byte[] bytes = Double.toString(unpackDouble).getBytes();
      floatBuffer.position(0);
      floatBuffer.put(bytes);
      floatBuffer.position(0);
      floatBuffer.limit(bytes.length);
      writeAsVarChar(floatBuffer, mapWriter, fieldName, listWriter);
    }
      break;
    case VARBINARY: {
      byte[] bytes = Double.toString(unpackDouble).getBytes();
      floatBuffer.position(0);
      floatBuffer.put(bytes);
      floatBuffer.position(0);
      floatBuffer.limit(bytes.length);
      writeAsVarBinary(floatBuffer, mapWriter, fieldName, listWriter);
    }
      break;
    case FLOAT8:
      writeAsFloat8(unpackDouble, mapWriter, fieldName, listWriter);
      break;
    case BIGINT:
      writeAsBigInt((long) unpackDouble, mapWriter, fieldName, listWriter);
      break;
    default:
      throw new DrillRuntimeException("Can't cast " + getDefaultType() + " into " + targetSchemaType);
    }
  }
}
