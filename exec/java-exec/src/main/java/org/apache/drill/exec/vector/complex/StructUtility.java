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

public class StructUtility {
  private final static String TYPE_MISMATCH_ERROR = " does not support heterogeneous value types. All values in the input map must be of the same type. The field [%s] has a differing type [%s].";

  /*
   * Function to read a value from the field reader, detect the type, construct the appropriate value holder
   * and use the value holder to write to the Map.
   */
  // TODO : This should be templatized and generated using freemarker
  public static void writeToStructFromReader(FieldReader fieldReader, BaseWriter.StructWriter structWriter, String caller) {
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
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).tinyInt());
          } else {
            fieldReader.copyAsValue(structWriter.tinyInt(MappifyUtility.fieldValue));
          }
          break;
        case SMALLINT:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).smallInt());
          } else {
            fieldReader.copyAsValue(structWriter.smallInt(MappifyUtility.fieldValue));
          }
          break;
        case BIGINT:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).bigInt());
          } else {
            fieldReader.copyAsValue(structWriter.bigInt(MappifyUtility.fieldValue));
          }
          break;
        case INT:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).integer());
          } else {
            fieldReader.copyAsValue(structWriter.integer(MappifyUtility.fieldValue));
          }
          break;
        case UINT1:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).uInt1());
          } else {
            fieldReader.copyAsValue(structWriter.uInt1(MappifyUtility.fieldValue));
          }
          break;
        case UINT2:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).uInt2());
          } else {
            fieldReader.copyAsValue(structWriter.uInt2(MappifyUtility.fieldValue));
          }
          break;
        case UINT4:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).uInt4());
          } else {
            fieldReader.copyAsValue(structWriter.uInt4(MappifyUtility.fieldValue));
          }
          break;
        case UINT8:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).uInt8());
          } else {
            fieldReader.copyAsValue(structWriter.uInt8(MappifyUtility.fieldValue));
          }
          break;
        case DECIMAL9:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).decimal9());
          } else {
            fieldReader.copyAsValue(structWriter.decimal9(MappifyUtility.fieldValue));
          }
          break;
        case DECIMAL18:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).decimal18());
          } else {
            fieldReader.copyAsValue(structWriter.decimal18(MappifyUtility.fieldValue));
          }
          break;
        case DECIMAL28SPARSE:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).decimal28Sparse());
          } else {
            fieldReader.copyAsValue(structWriter.decimal28Sparse(MappifyUtility.fieldValue));
          }
          break;
        case DECIMAL38SPARSE:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).decimal38Sparse());
          } else {
            fieldReader.copyAsValue(structWriter.decimal38Sparse(MappifyUtility.fieldValue));
          }
          break;
        case VARDECIMAL:
          if (repeated) {
            fieldReader.copyAsValue(
                structWriter.list(MappifyUtility.fieldValue)
                    .varDecimal(valueMajorType.getScale(), valueMajorType.getPrecision()));
          } else {
            fieldReader.copyAsValue(
                structWriter.varDecimal(MappifyUtility.fieldValue, valueMajorType.getScale(), valueMajorType.getPrecision()));
          }
          break;
        case DATE:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).date());
          } else {
            fieldReader.copyAsValue(structWriter.date(MappifyUtility.fieldValue));
          }
          break;
        case TIME:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).time());
          } else {
            fieldReader.copyAsValue(structWriter.time(MappifyUtility.fieldValue));
          }
          break;
        case TIMESTAMP:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).timeStamp());
          } else {
            fieldReader.copyAsValue(structWriter.timeStamp(MappifyUtility.fieldValue));
          }
          break;
        case INTERVAL:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).interval());
          } else {
            fieldReader.copyAsValue(structWriter.interval(MappifyUtility.fieldValue));
          }
          break;
        case INTERVALDAY:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).intervalDay());
          } else {
            fieldReader.copyAsValue(structWriter.intervalDay(MappifyUtility.fieldValue));
          }
          break;
        case INTERVALYEAR:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).intervalYear());
          } else {
            fieldReader.copyAsValue(structWriter.intervalYear(MappifyUtility.fieldValue));
          }
          break;
        case FLOAT4:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).float4());
          } else {
            fieldReader.copyAsValue(structWriter.float4(MappifyUtility.fieldValue));
          }
          break;
        case FLOAT8:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).float8());
          } else {
            fieldReader.copyAsValue(structWriter.float8(MappifyUtility.fieldValue));
          }
          break;
        case BIT:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).bit());
          } else {
            fieldReader.copyAsValue(structWriter.bit(MappifyUtility.fieldValue));
          }
          break;
        case VARCHAR:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).varChar());
          } else {
            fieldReader.copyAsValue(structWriter.varChar(MappifyUtility.fieldValue));
          }
          break;
        case VARBINARY:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).varBinary());
          } else {
            fieldReader.copyAsValue(structWriter.varBinary(MappifyUtility.fieldValue));
          }
          break;
        case STRUCT:
          if (valueMajorType.getMode() == TypeProtos.DataMode.REPEATED) {
            fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).struct());
          } else {
            fieldReader.copyAsValue(structWriter.struct(MappifyUtility.fieldValue));
          }
          break;
        case LIST:
          fieldReader.copyAsValue(structWriter.list(MappifyUtility.fieldValue).list());
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

  public static void writeToStructFromReader(FieldReader fieldReader, BaseWriter.StructWriter structWriter,
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
            fieldReader.copyAsValue(structWriter.list(fieldName).tinyInt());
          } else {
            fieldReader.copyAsValue(structWriter.tinyInt(fieldName));
          }
          break;
        case SMALLINT:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(fieldName).smallInt());
          } else {
            fieldReader.copyAsValue(structWriter.smallInt(fieldName));
          }
          break;
        case BIGINT:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(fieldName).bigInt());
          } else {
            fieldReader.copyAsValue(structWriter.bigInt(fieldName));
          }
          break;
        case INT:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(fieldName).integer());
          } else {
            fieldReader.copyAsValue(structWriter.integer(fieldName));
          }
          break;
        case UINT1:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(fieldName).uInt1());
          } else {
            fieldReader.copyAsValue(structWriter.uInt1(fieldName));
          }
          break;
        case UINT2:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(fieldName).uInt2());
          } else {
            fieldReader.copyAsValue(structWriter.uInt2(fieldName));
          }
          break;
        case UINT4:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(fieldName).uInt4());
          } else {
            fieldReader.copyAsValue(structWriter.uInt4(fieldName));
          }
          break;
        case UINT8:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(fieldName).uInt8());
          } else {
            fieldReader.copyAsValue(structWriter.uInt8(fieldName));
          }
          break;
        case DECIMAL9:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(fieldName).decimal9());
          } else {
            fieldReader.copyAsValue(structWriter.decimal9(fieldName));
          }
          break;
        case DECIMAL18:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(fieldName).decimal18());
          } else {
            fieldReader.copyAsValue(structWriter.decimal18(fieldName));
          }
          break;
        case DECIMAL28SPARSE:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(fieldName).decimal28Sparse());
          } else {
            fieldReader.copyAsValue(structWriter.decimal28Sparse(fieldName));
          }
          break;
        case DECIMAL38SPARSE:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(fieldName).decimal38Sparse());
          } else {
            fieldReader.copyAsValue(structWriter.decimal38Sparse(fieldName));
          }
          break;
        case VARDECIMAL:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(fieldName).varDecimal(valueMajorType.getScale(), valueMajorType.getPrecision()));
          } else {
            fieldReader.copyAsValue(structWriter.varDecimal(fieldName, valueMajorType.getScale(), valueMajorType.getPrecision()));
          }
          break;
        case DATE:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(fieldName).date());
          } else {
            fieldReader.copyAsValue(structWriter.date(fieldName));
          }
          break;
        case TIME:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(fieldName).time());
          } else {
            fieldReader.copyAsValue(structWriter.time(fieldName));
          }
          break;
        case TIMESTAMP:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(fieldName).timeStamp());
          } else {
            fieldReader.copyAsValue(structWriter.timeStamp(fieldName));
          }
          break;
        case INTERVAL:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(fieldName).interval());
          } else {
            fieldReader.copyAsValue(structWriter.interval(fieldName));
          }
          break;
        case INTERVALDAY:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(fieldName).intervalDay());
          } else {
            fieldReader.copyAsValue(structWriter.intervalDay(fieldName));
          }
          break;
        case INTERVALYEAR:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(fieldName).intervalYear());
          } else {
            fieldReader.copyAsValue(structWriter.intervalYear(fieldName));
          }
          break;
        case FLOAT4:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(fieldName).float4());
          } else {
            fieldReader.copyAsValue(structWriter.float4(fieldName));
          }
          break;
        case FLOAT8:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(fieldName).float8());
          } else {
            fieldReader.copyAsValue(structWriter.float8(fieldName));
          }
          break;
        case BIT:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(fieldName).bit());
          } else {
            fieldReader.copyAsValue(structWriter.bit(fieldName));
          }
          break;
        case VARCHAR:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(fieldName).varChar());
          } else {
            fieldReader.copyAsValue(structWriter.varChar(fieldName));
          }
          break;
        case VARBINARY:
          if (repeated) {
            fieldReader.copyAsValue(structWriter.list(fieldName).varBinary());
          } else {
            fieldReader.copyAsValue(structWriter.varBinary(fieldName));
          }
          break;
        case STRUCT:
          if (valueMajorType.getMode() == TypeProtos.DataMode.REPEATED) {
            fieldReader.copyAsValue(structWriter.list(fieldName).struct());
          } else {
            fieldReader.copyAsValue(structWriter.struct(fieldName));
          }
          break;
        case LIST:
          fieldReader.copyAsValue(structWriter.list(fieldName).list());
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
      boolean repeated = false;

      if (valueMajorType.getMode() == TypeProtos.DataMode.REPEATED) {
        repeated = true;
      }

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
          fieldReader.copyAsValue(listWriter.varDecimal(valueMajorType.getScale(), valueMajorType.getPrecision()));
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
        case STRUCT:
          fieldReader.copyAsValue(listWriter.struct());
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
