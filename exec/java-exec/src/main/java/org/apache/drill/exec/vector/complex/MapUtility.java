/**
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
package org.apache.drill.exec.vector.complex;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.TypeProtos.MajorType;

import org.apache.drill.exec.expr.fn.impl.MappifyUtility;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

public class MapUtility {
  private final static String TYPE_MISMATCH_ERROR = "Mappify/kvgen does not support heterogeneous value types. All values in the input map must be of the same type. The field [%s] has a differing type [%s].";

  /*
   * Function to read a value from the field reader, detect the type, construct the appropriate value holder
   * and use the value holder to write to the Map.
   */
  // TODO : This should be templatized and generated using freemarker
  public static void writeToMapFromReader(FieldReader fieldReader, BaseWriter.MapWriter mapWriter) {
    try {
      MajorType valueMajorType = fieldReader.getType();
      MinorType valueMinorType = valueMajorType.getMinorType();
      boolean repeated = false;

      if (valueMajorType.getMode() == TypeProtos.DataMode.REPEATED) {
        repeated = true;
      }

      switch (valueMinorType) {
        case TINYINT:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).tinyInt());
          } else {
            fieldReader.copyAsValue(mapWriter.tinyInt(MappifyUtility.fieldValue));
          }
          break;
        case SMALLINT:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).smallInt());
          } else {
            fieldReader.copyAsValue(mapWriter.smallInt(MappifyUtility.fieldValue));
          }
          break;
        case BIGINT:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).bigInt());
          } else {
            fieldReader.copyAsValue(mapWriter.bigInt(MappifyUtility.fieldValue));
          }
          break;
        case INT:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).integer());
          } else {
            fieldReader.copyAsValue(mapWriter.integer(MappifyUtility.fieldValue));
          }
          break;
        case UINT1:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).uInt1());
          } else {
            fieldReader.copyAsValue(mapWriter.uInt1(MappifyUtility.fieldValue));
          }
          break;
        case UINT2:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).uInt2());
          } else {
            fieldReader.copyAsValue(mapWriter.uInt2(MappifyUtility.fieldValue));
          }
          break;
        case UINT4:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).uInt4());
          } else {
            fieldReader.copyAsValue(mapWriter.uInt4(MappifyUtility.fieldValue));
          }
          break;
        case UINT8:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).uInt8());
          } else {
            fieldReader.copyAsValue(mapWriter.uInt8(MappifyUtility.fieldValue));
          }
          break;
        case DECIMAL9:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).decimal9());
          } else {
            fieldReader.copyAsValue(mapWriter.decimal9(MappifyUtility.fieldValue));
          }
          break;
        case DECIMAL18:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).decimal18());
          } else {
            fieldReader.copyAsValue(mapWriter.decimal18(MappifyUtility.fieldValue));
          }
          break;
        case DECIMAL28SPARSE:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).decimal28Sparse());
          } else {
            fieldReader.copyAsValue(mapWriter.decimal28Sparse(MappifyUtility.fieldValue));
          }
          break;
        case DECIMAL38SPARSE:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).decimal38Sparse());
          } else {
            fieldReader.copyAsValue(mapWriter.decimal38Sparse(MappifyUtility.fieldValue));
          }
          break;
        case DATE:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).date());
          } else {
            fieldReader.copyAsValue(mapWriter.date(MappifyUtility.fieldValue));
          }
          break;
        case TIME:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).time());
          } else {
            fieldReader.copyAsValue(mapWriter.time(MappifyUtility.fieldValue));
          }
          break;
        case TIMESTAMP:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).timeStamp());
          } else {
            fieldReader.copyAsValue(mapWriter.timeStamp(MappifyUtility.fieldValue));
          }
          break;
        case INTERVAL:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).interval());
          } else {
            fieldReader.copyAsValue(mapWriter.interval(MappifyUtility.fieldValue));
          }
          break;
        case INTERVALDAY:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).intervalDay());
          } else {
            fieldReader.copyAsValue(mapWriter.intervalDay(MappifyUtility.fieldValue));
          }
          break;
        case INTERVALYEAR:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).intervalYear());
          } else {
            fieldReader.copyAsValue(mapWriter.intervalYear(MappifyUtility.fieldValue));
          }
          break;
        case FLOAT4:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).float4());
          } else {
            fieldReader.copyAsValue(mapWriter.float4(MappifyUtility.fieldValue));
          }
          break;
        case FLOAT8:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).float8());
          } else {
            fieldReader.copyAsValue(mapWriter.float8(MappifyUtility.fieldValue));
          }
          break;
        case BIT:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).bit());
          } else {
            fieldReader.copyAsValue(mapWriter.bit(MappifyUtility.fieldValue));
          }
          break;
        case VARCHAR:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).varChar());
          } else {
            fieldReader.copyAsValue(mapWriter.varChar(MappifyUtility.fieldValue));
          }
          break;
        case VARBINARY:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).varBinary());
          } else {
            fieldReader.copyAsValue(mapWriter.varBinary(MappifyUtility.fieldValue));
          }
          break;
        case MAP:
          if (valueMajorType.getMode() == TypeProtos.DataMode.REPEATED) {
            fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).map());
          } else {
            fieldReader.copyAsValue(mapWriter.map(MappifyUtility.fieldValue));
          }
          break;
        case LIST:
          fieldReader.copyAsValue(mapWriter.list(MappifyUtility.fieldValue).list());
          break;
        default:
          throw new DrillRuntimeException(String.format("kvgen does not support input of type: %s", valueMinorType));
      }
    } catch (ClassCastException e) {
      final MaterializedField field = fieldReader.getField();
      throw new DrillRuntimeException(String.format(TYPE_MISMATCH_ERROR, field.getPath(), field.getType()));
    }
  }
}
