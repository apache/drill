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

import java.math.BigInteger;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.Value;

public class IntegerValueWriter extends ScalarValueWriter {

  public IntegerValueWriter() {
  }

  @Override
  public void write(Value v, MapWriter mapWriter, String fieldName, ListWriter listWriter, FieldSelection selection,
      MaterializedField schema) {

    IntegerValue value = v.asIntegerValue();
    if (!value.isInLongRange()) {
      BigInteger i = value.toBigInteger();
      throw new DrillRuntimeException(
          "UnSupported messagepack type: " + value.getValueType() + " with BigInteger value: " + i);
    }

    MinorType targetSchemaType = getTargetType(MinorType.BIGINT, schema);
    if (logger.isDebugEnabled()) {
      log(v,mapWriter, fieldName, listWriter, selection, targetSchemaType);
    }
   switch (targetSchemaType) {
    case VARCHAR:
      String s = value.toString();
      writeAsVarChar(s.getBytes(), mapWriter, fieldName, listWriter);
      break;
    case VARBINARY:
      long longValue = value.toLong();
      ensure(8);
      context.workBuf.setLong(0, longValue);
      writeAsVarBinary(mapWriter, fieldName, listWriter, 8);
      break;
    case BIGINT:
      writeAsBigInt(value.toLong(), mapWriter, fieldName, listWriter);
      break;
    default:
      throw new DrillRuntimeException("Can't cast " + value.getValueType() + " into " + targetSchemaType);
    }
  }

}
