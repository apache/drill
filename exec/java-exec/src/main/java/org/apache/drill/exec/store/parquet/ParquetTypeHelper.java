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
package org.apache.drill.exec.store.parquet;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Maps;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;

import java.util.Map;


/**
 * Util class for converting Parquet to Drill data types and vise versa.
 */
public class ParquetTypeHelper {

  private static Map<TypeProtos.MinorType, PrimitiveTypeName> typeMap;
  private static ImmutableBiMap<TypeProtos.DataMode, Repetition> modeMap;
  private static Map<TypeProtos.MinorType, OriginalType> originalTypeMap;

  static {
    typeMap = Maps.newHashMap();

    typeMap.put(TypeProtos.MinorType.TINYINT, PrimitiveTypeName.INT32);
    typeMap.put(TypeProtos.MinorType.UINT1, PrimitiveTypeName.INT32);
    typeMap.put(TypeProtos.MinorType.UINT2, PrimitiveTypeName.INT32);
    typeMap.put(TypeProtos.MinorType.SMALLINT, PrimitiveTypeName.INT32);
    typeMap.put(TypeProtos.MinorType.INT, PrimitiveTypeName.INT32);
    typeMap.put(TypeProtos.MinorType.UINT4, PrimitiveTypeName.INT32);
    typeMap.put(TypeProtos.MinorType.FLOAT4, PrimitiveTypeName.FLOAT);
    typeMap.put(TypeProtos.MinorType.TIME, PrimitiveTypeName.INT32);
    typeMap.put(TypeProtos.MinorType.INTERVALYEAR, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
    typeMap.put(TypeProtos.MinorType.DECIMAL9, PrimitiveTypeName.INT32);
    typeMap.put(TypeProtos.MinorType.BIGINT, PrimitiveTypeName.INT64);
    typeMap.put(TypeProtos.MinorType.UINT8, PrimitiveTypeName.INT64);
    typeMap.put(TypeProtos.MinorType.FLOAT8, PrimitiveTypeName.DOUBLE);
    typeMap.put(TypeProtos.MinorType.DATE, PrimitiveTypeName.INT32);
    typeMap.put(TypeProtos.MinorType.TIMESTAMP, PrimitiveTypeName.INT64);
    typeMap.put(TypeProtos.MinorType.DECIMAL18, PrimitiveTypeName.INT64);
    typeMap.put(TypeProtos.MinorType.INTERVALDAY, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
    typeMap.put(TypeProtos.MinorType.INTERVAL, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
    typeMap.put(TypeProtos.MinorType.DECIMAL28DENSE, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
    typeMap.put(TypeProtos.MinorType.DECIMAL38DENSE, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
    typeMap.put(TypeProtos.MinorType.DECIMAL38SPARSE, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
    typeMap.put(TypeProtos.MinorType.DECIMAL28SPARSE, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
    typeMap.put(TypeProtos.MinorType.VARBINARY, PrimitiveTypeName.BINARY);
    typeMap.put(TypeProtos.MinorType.VARCHAR, PrimitiveTypeName.BINARY);
    typeMap.put(TypeProtos.MinorType.VAR16CHAR, PrimitiveTypeName.BINARY);
    typeMap.put(TypeProtos.MinorType.BIT, PrimitiveTypeName.BOOLEAN);

    modeMap = new ImmutableBiMap.Builder<TypeProtos.DataMode, Repetition>()
            .put(TypeProtos.DataMode.REQUIRED, Repetition.REQUIRED)
            .put(TypeProtos.DataMode.OPTIONAL, Repetition.OPTIONAL)
            .put(TypeProtos.DataMode.REPEATED, Repetition.REPEATED)
            .build();

    originalTypeMap = Maps.newHashMap();

    originalTypeMap.put(TypeProtos.MinorType.DECIMAL9, OriginalType.DECIMAL);
    originalTypeMap.put(TypeProtos.MinorType.DECIMAL18, OriginalType.DECIMAL);
    originalTypeMap.put(TypeProtos.MinorType.DECIMAL28DENSE, OriginalType.DECIMAL);
    originalTypeMap.put(TypeProtos.MinorType.DECIMAL38DENSE, OriginalType.DECIMAL);
    originalTypeMap.put(TypeProtos.MinorType.DECIMAL38SPARSE, OriginalType.DECIMAL);
    originalTypeMap.put(TypeProtos.MinorType.DECIMAL28SPARSE, OriginalType.DECIMAL);
    originalTypeMap.put(TypeProtos.MinorType.VARCHAR, OriginalType.UTF8);
    originalTypeMap.put(TypeProtos.MinorType.DATE, OriginalType.DATE);
    originalTypeMap.put(TypeProtos.MinorType.TIME, OriginalType.TIME_MILLIS);
    originalTypeMap.put(TypeProtos.MinorType.TIMESTAMP, OriginalType.TIMESTAMP_MILLIS);
    originalTypeMap.put(TypeProtos.MinorType.INTERVALDAY, OriginalType.INTERVAL);
    originalTypeMap.put(TypeProtos.MinorType.INTERVALYEAR, OriginalType.INTERVAL);
    originalTypeMap.put(TypeProtos.MinorType.INTERVAL, OriginalType.INTERVAL);
  }

  /**
   * @param minorType Drill minor type
   * @return Parquet primitive type in correspondence to Drill minor type
   */
  public static PrimitiveTypeName getPrimitiveTypeNameForMinorType(TypeProtos.MinorType minorType) {
    return typeMap.get(minorType);
  }

  /**
   * @param dataMode Drill data mode
   * @return Parquet repetition type in correspondence to Drill data mode type
   */
  public static Repetition getRepetitionForDataMode(TypeProtos.DataMode dataMode) {
    return modeMap.get(dataMode);
  }

  /**
   * @param repetition Parquet repetition type
   * @return Drill data mode type in correspondence to to Parquet repetition type
   */
  public static TypeProtos.DataMode getDataModeForRepetition(Repetition repetition) {
    return modeMap.inverse().get(repetition);
  }

  /**
   * @param minorType Drill minor type
   * @return // Parquet original type in correspondence to Drill minor type
   */
  public static OriginalType getOriginalTypeForMinorType(TypeProtos.MinorType minorType) {
    return originalTypeMap.get(minorType);
  }

  /**
   * @param field Drill materialized field
   * @return Parquet decimal metadata for Drill materialized field
   */
  public static DecimalMetadata getDecimalMetadataForField(MaterializedField field) {
    switch(field.getType().getMinorType()) {
      case DECIMAL9:
      case DECIMAL18:
      case DECIMAL28SPARSE:
      case DECIMAL28DENSE:
      case DECIMAL38SPARSE:
      case DECIMAL38DENSE:
        return new DecimalMetadata(field.getPrecision(), field.getScale());
      default:
        return null;
    }
  }

  /**
   * @param minorType Drill minor type
   * @return length for minor types in bytes
   */
  public static int getLengthForMinorType(TypeProtos.MinorType minorType) {
    switch(minorType) {
      case INTERVALDAY:
      case INTERVALYEAR:
      case INTERVAL:
        return 12;
      case DECIMAL28SPARSE:
        return 12;
      case DECIMAL38SPARSE:
        return 16;
      default:
        return 0;
    }
  }

}
