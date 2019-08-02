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
  private final static String TYPE_MISMATCH_ERROR = " does not support heterogeneous value types. All values in the input map must be of the same type. The field [%s] has a differing type [%s].";

  /*
   * Function to read a value from the field reader, detect the type, construct the appropriate value holder
   * and use the value holder to write to the Map.
   */
  // TODO : This should be templatized and generated using freemarker
  public static void writeToMapFromReader(FieldReader fieldReader, BaseWriter.MapWriter mapWriter, String caller) {
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
        case VARDECIMAL:
          if (repeated) {
            fieldReader.copyAsValue(
                mapWriter.list(MappifyUtility.fieldValue)
                    .varDecimal(valueMajorType.getPrecision(), valueMajorType.getScale()));
          } else {
            fieldReader.copyAsValue(
                mapWriter.varDecimal(MappifyUtility.fieldValue, valueMajorType.getPrecision(), valueMajorType.getScale()));
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
          throw new DrillRuntimeException(String.format(caller
              + " does not support input of type: %s", valueMinorType));
      }
    } catch (ClassCastException e) {
      final MaterializedField field = fieldReader.getField();
      throw new DrillRuntimeException(String.format(caller + TYPE_MISMATCH_ERROR, field.getName(), field.getType()));
    }
  }

  public static void writeToMapFromReader(FieldReader fieldReader, BaseWriter.MapWriter mapWriter,
      String fieldName, String caller) {
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
            fieldReader.copyAsValue(mapWriter.list(fieldName).tinyInt());
          } else {
            fieldReader.copyAsValue(mapWriter.tinyInt(fieldName));
          }
          break;
        case SMALLINT:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).smallInt());
          } else {
            fieldReader.copyAsValue(mapWriter.smallInt(fieldName));
          }
          break;
        case BIGINT:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).bigInt());
          } else {
            fieldReader.copyAsValue(mapWriter.bigInt(fieldName));
          }
          break;
        case INT:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).integer());
          } else {
            fieldReader.copyAsValue(mapWriter.integer(fieldName));
          }
          break;
        case UINT1:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).uInt1());
          } else {
            fieldReader.copyAsValue(mapWriter.uInt1(fieldName));
          }
          break;
        case UINT2:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).uInt2());
          } else {
            fieldReader.copyAsValue(mapWriter.uInt2(fieldName));
          }
          break;
        case UINT4:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).uInt4());
          } else {
            fieldReader.copyAsValue(mapWriter.uInt4(fieldName));
          }
          break;
        case UINT8:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).uInt8());
          } else {
            fieldReader.copyAsValue(mapWriter.uInt8(fieldName));
          }
          break;
        case DECIMAL9:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).decimal9());
          } else {
            fieldReader.copyAsValue(mapWriter.decimal9(fieldName));
          }
          break;
        case DECIMAL18:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).decimal18());
          } else {
            fieldReader.copyAsValue(mapWriter.decimal18(fieldName));
          }
          break;
        case DECIMAL28SPARSE:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).decimal28Sparse());
          } else {
            fieldReader.copyAsValue(mapWriter.decimal28Sparse(fieldName));
          }
          break;
        case DECIMAL38SPARSE:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).decimal38Sparse());
          } else {
            fieldReader.copyAsValue(mapWriter.decimal38Sparse(fieldName));
          }
          break;
        case VARDECIMAL:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).varDecimal(valueMajorType.getPrecision(), valueMajorType.getScale()));
          } else {
            fieldReader.copyAsValue(mapWriter.varDecimal(fieldName, valueMajorType.getPrecision(), valueMajorType.getScale()));
          }
          break;
        case DATE:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).date());
          } else {
            fieldReader.copyAsValue(mapWriter.date(fieldName));
          }
          break;
        case TIME:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).time());
          } else {
            fieldReader.copyAsValue(mapWriter.time(fieldName));
          }
          break;
        case TIMESTAMP:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).timeStamp());
          } else {
            fieldReader.copyAsValue(mapWriter.timeStamp(fieldName));
          }
          break;
        case INTERVAL:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).interval());
          } else {
            fieldReader.copyAsValue(mapWriter.interval(fieldName));
          }
          break;
        case INTERVALDAY:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).intervalDay());
          } else {
            fieldReader.copyAsValue(mapWriter.intervalDay(fieldName));
          }
          break;
        case INTERVALYEAR:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).intervalYear());
          } else {
            fieldReader.copyAsValue(mapWriter.intervalYear(fieldName));
          }
          break;
        case FLOAT4:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).float4());
          } else {
            fieldReader.copyAsValue(mapWriter.float4(fieldName));
          }
          break;
        case FLOAT8:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).float8());
          } else {
            fieldReader.copyAsValue(mapWriter.float8(fieldName));
          }
          break;
        case BIT:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).bit());
          } else {
            fieldReader.copyAsValue(mapWriter.bit(fieldName));
          }
          break;
        case VARCHAR:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).varChar());
          } else {
            fieldReader.copyAsValue(mapWriter.varChar(fieldName));
          }
          break;
        case VARBINARY:
          if (repeated) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).varBinary());
          } else {
            fieldReader.copyAsValue(mapWriter.varBinary(fieldName));
          }
          break;
        case MAP:
          if (valueMajorType.getMode() == TypeProtos.DataMode.REPEATED) {
            fieldReader.copyAsValue(mapWriter.list(fieldName).map());
          } else {
            fieldReader.copyAsValue(mapWriter.map(fieldName));
          }
          break;
        case LIST:
          fieldReader.copyAsValue(mapWriter.list(fieldName).list());
          break;
        default:
          throw new DrillRuntimeException(String.format(caller
              + " does not support input of type: %s", valueMinorType));
      }
    } catch (ClassCastException e) {
      final MaterializedField field = fieldReader.getField();
      throw new DrillRuntimeException(String.format(caller + TYPE_MISMATCH_ERROR, field.getName(), field.getType()));
    }
  }

  public static void writeToListFromReader(FieldReader fieldReader, BaseWriter.ListWriter listWriter, String caller) {
    try {
      MajorType valueMajorType = fieldReader.getType();
      MinorType valueMinorType = valueMajorType.getMinorType();

      switch (valueMinorType) {
        case TINYINT:
          fieldReader.copyAsValue(listWriter.tinyInt());
          break;
        case SMALLINT:
          fieldReader.copyAsValue(listWriter.smallInt());
          break;
        case BIGINT:
          fieldReader.copyAsValue(listWriter.bigInt());
          break;
        case INT:
          fieldReader.copyAsValue(listWriter.integer());
          break;
        case UINT1:
          fieldReader.copyAsValue(listWriter.uInt1());
          break;
        case UINT2:
          fieldReader.copyAsValue(listWriter.uInt2());
          break;
        case UINT4:
          fieldReader.copyAsValue(listWriter.uInt4());
          break;
        case UINT8:
          fieldReader.copyAsValue(listWriter.uInt8());
          break;
        case DECIMAL9:
          fieldReader.copyAsValue(listWriter.decimal9());
          break;
        case DECIMAL18:
          fieldReader.copyAsValue(listWriter.decimal18());
          break;
        case DECIMAL28SPARSE:
          fieldReader.copyAsValue(listWriter.decimal28Sparse());
          break;
        case DECIMAL38SPARSE:
          fieldReader.copyAsValue(listWriter.decimal38Sparse());
          break;
        case VARDECIMAL:
          fieldReader.copyAsValue(listWriter.varDecimal(valueMajorType.getPrecision(), valueMajorType.getScale()));
          break;
        case DATE:
          fieldReader.copyAsValue(listWriter.date());
          break;
        case TIME:
          fieldReader.copyAsValue(listWriter.time());
          break;
        case TIMESTAMP:
          fieldReader.copyAsValue(listWriter.timeStamp());
          break;
        case INTERVAL:
          fieldReader.copyAsValue(listWriter.interval());
          break;
        case INTERVALDAY:
          fieldReader.copyAsValue(listWriter.intervalDay());
          break;
        case INTERVALYEAR:
          fieldReader.copyAsValue(listWriter.intervalYear());
          break;
        case FLOAT4:
          fieldReader.copyAsValue(listWriter.float4());
          break;
        case FLOAT8:
          fieldReader.copyAsValue(listWriter.float8());
          break;
        case BIT:
          fieldReader.copyAsValue(listWriter.bit());
          break;
        case VARCHAR:
          fieldReader.copyAsValue(listWriter.varChar());
          break;
        case VARBINARY:
          fieldReader.copyAsValue(listWriter.varBinary());
          break;
        case MAP:
          fieldReader.copyAsValue(listWriter.map());
          break;
        case LIST:
          fieldReader.copyAsValue(listWriter.list());
          break;
        default:
          throw new DrillRuntimeException(String.format(caller
              + " function does not support input of type: %s", valueMinorType));
      }
    } catch (ClassCastException e) {
      final MaterializedField field = fieldReader.getField();
      throw new DrillRuntimeException(String.format(caller + TYPE_MISMATCH_ERROR, field.getName(), field.getType()));
    }
  }
}
