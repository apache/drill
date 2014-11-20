/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.store.parquet.columnreaders;

import static parquet.Preconditions.checkArgument;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;

import parquet.format.ConvertedType;
import parquet.format.SchemaElement;
import parquet.schema.PrimitiveType;

public class ParquetToDrillTypeConverter {

  private static TypeProtos.MinorType getDecimalType(SchemaElement schemaElement) {
    return schemaElement.getPrecision() <= 28 ? TypeProtos.MinorType.DECIMAL28SPARSE : MinorType.DECIMAL38SPARSE;
  }

  public static TypeProtos.MajorType toMajorType(PrimitiveType.PrimitiveTypeName primitiveTypeName, int length,
                                          TypeProtos.DataMode mode, SchemaElement schemaElement) {
    ConvertedType convertedType = schemaElement.getConverted_type();
    switch (mode) {

      case OPTIONAL:
        switch (primitiveTypeName) {
          case BINARY:
            if (convertedType == null) {
              return Types.optional(TypeProtos.MinorType.VARBINARY);
            }
            switch (convertedType) {
              case UTF8:
                return Types.optional(TypeProtos.MinorType.VARCHAR);
              case DECIMAL:
                return Types.withScaleAndPrecision(getDecimalType(schemaElement), TypeProtos.DataMode.OPTIONAL, schemaElement.getScale(), schemaElement.getPrecision());
              default:
                return Types.optional(TypeProtos.MinorType.VARBINARY);
            }
          case INT64:
            if (convertedType == null) {
              return Types.optional(TypeProtos.MinorType.BIGINT);
            }
            switch(convertedType) {
              case DECIMAL:
                return Types.withScaleAndPrecision(TypeProtos.MinorType.DECIMAL18, DataMode.OPTIONAL, schemaElement.getScale(), schemaElement.getPrecision());
              // TODO - add this back if it is decided to be added upstream, was removed form our pull request July 2014
//              case TIME_MICROS:
//                throw new UnsupportedOperationException();
              case TIMESTAMP_MILLIS:
                return Types.optional(MinorType.TIMESTAMP);
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, convertedType));
            }
          case INT32:
            if (convertedType == null) {
              return Types.optional(TypeProtos.MinorType.INT);
            }
            switch(convertedType) {
              case DECIMAL:
                return Types.withScaleAndPrecision(MinorType.DECIMAL9, DataMode.OPTIONAL, schemaElement.getScale(), schemaElement.getPrecision());
              case DATE:
                return Types.optional(MinorType.DATE);
              case TIME_MILLIS:
                return Types.optional(MinorType.TIME);
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, convertedType));
            }
          case BOOLEAN:
            return Types.optional(TypeProtos.MinorType.BIT);
          case FLOAT:
            return Types.optional(TypeProtos.MinorType.FLOAT4);
          case DOUBLE:
            return Types.optional(TypeProtos.MinorType.FLOAT8);
          // TODO - Both of these are not supported by the parquet library yet (7/3/13),
          // but they are declared here for when they are implemented
          case INT96:
            return TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.FIXEDBINARY).setWidth(12)
                .setMode(mode).build();
          case FIXED_LEN_BYTE_ARRAY:
            if (convertedType == null) {
              checkArgument(length > 0, "A length greater than zero must be provided for a FixedBinary type.");
              return TypeProtos.MajorType.newBuilder().setMinorType(MinorType.VARBINARY).setMode(mode).build();
            } else if (convertedType == ConvertedType.DECIMAL) {
              return Types.withScaleAndPrecision(getDecimalType(schemaElement), DataMode.OPTIONAL, schemaElement.getScale(), schemaElement.getPrecision());
            }
          default:
            throw new UnsupportedOperationException("Type not supported: " + primitiveTypeName);
        }
      case REQUIRED:
        switch (primitiveTypeName) {
          case BINARY:
            if (convertedType == null) {
              return Types.required(TypeProtos.MinorType.VARBINARY);
            }
            switch (convertedType) {
              case UTF8:
                return Types.required(MinorType.VARCHAR);
              case DECIMAL:
                return Types.withScaleAndPrecision(getDecimalType(schemaElement), DataMode.REQUIRED, schemaElement.getScale(), schemaElement.getPrecision());
              default:
                return Types.required(TypeProtos.MinorType.VARBINARY);
            }
          case INT64:
            if (convertedType == null) {
              return Types.required(MinorType.BIGINT);
            }
            switch(convertedType) {
              case DECIMAL:
                return Types.withScaleAndPrecision(MinorType.DECIMAL18, DataMode.REQUIRED, schemaElement.getScale(), schemaElement.getPrecision());
//              case FINETIME:
//                throw new UnsupportedOperationException();
              case TIMESTAMP_MILLIS:
                return Types.required(MinorType.TIMESTAMP);
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, convertedType));
            }
          case INT32:
            if (convertedType == null) {
              return Types.required(MinorType.INT);
            }
            switch(convertedType) {
              case DECIMAL:
                return Types.withScaleAndPrecision(MinorType.DECIMAL9, DataMode.REQUIRED, schemaElement.getScale(), schemaElement.getPrecision());
              case DATE:
                return Types.required(MinorType.DATE);
              case TIME_MILLIS:
                return Types.required(MinorType.TIME);
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, convertedType));
            }
          case BOOLEAN:
            return Types.required(TypeProtos.MinorType.BIT);
          case FLOAT:
            return Types.required(TypeProtos.MinorType.FLOAT4);
          case DOUBLE:
            return Types.required(TypeProtos.MinorType.FLOAT8);
          // Both of these are not supported by the parquet library yet (7/3/13),
          // but they are declared here for when they are implemented
          case INT96:
            return TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.FIXEDBINARY).setWidth(12)
                .setMode(mode).build();
          case FIXED_LEN_BYTE_ARRAY:
            if (convertedType == null) {
              checkArgument(length > 0, "A length greater than zero must be provided for a FixedBinary type.");
              return TypeProtos.MajorType.newBuilder().setMinorType(MinorType.VARBINARY).setMode(mode).build();
            } else if (convertedType == ConvertedType.DECIMAL) {
              return Types.withScaleAndPrecision(getDecimalType(schemaElement), DataMode.REQUIRED, schemaElement.getScale(), schemaElement.getPrecision());
            }
          default:
            throw new UnsupportedOperationException("Type not supported: " + primitiveTypeName);
        }
      case REPEATED:
        switch (primitiveTypeName) {
          case BINARY:
            if (convertedType == null) {
              return Types.repeated(TypeProtos.MinorType.VARBINARY);
            }
            switch (schemaElement.getConverted_type()) {
              case UTF8:
                return Types.repeated(MinorType.VARCHAR);
              case DECIMAL:
                return Types.withScaleAndPrecision(getDecimalType(schemaElement), DataMode.REPEATED, schemaElement.getScale(), schemaElement.getPrecision());
              default:
                return Types.repeated(TypeProtos.MinorType.VARBINARY);
            }
          case INT64:
            if (convertedType == null) {
              return Types.repeated(MinorType.BIGINT);
            }
            switch(convertedType) {
              case DECIMAL:
                return Types.withScaleAndPrecision(MinorType.DECIMAL18, DataMode.REPEATED, schemaElement.getScale(), schemaElement.getPrecision());
//              case FINETIME:
//                throw new UnsupportedOperationException();
              case TIMESTAMP_MILLIS:
                return Types.repeated(MinorType.TIMESTAMP);
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, convertedType));
            }
          case INT32:
            if (convertedType == null) {
              return Types.repeated(MinorType.INT);
            }
            switch(convertedType) {
              case DECIMAL:
                return Types.withScaleAndPrecision(MinorType.DECIMAL9, DataMode.REPEATED, schemaElement.getScale(), schemaElement.getPrecision());
              case DATE:
                return Types.repeated(MinorType.DATE);
              case TIME_MILLIS:
                return Types.repeated(MinorType.TIME);
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, convertedType));
            }
          case BOOLEAN:
            return Types.repeated(TypeProtos.MinorType.BIT);
          case FLOAT:
            return Types.repeated(TypeProtos.MinorType.FLOAT4);
          case DOUBLE:
            return Types.repeated(TypeProtos.MinorType.FLOAT8);
          // Both of these are not supported by the parquet library yet (7/3/13),
          // but they are declared here for when they are implemented
          case INT96:
            return TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.FIXEDBINARY).setWidth(12)
                .setMode(mode).build();
          case FIXED_LEN_BYTE_ARRAY:
            if (convertedType == null) {
              checkArgument(length > 0, "A length greater than zero must be provided for a FixedBinary type.");
              return TypeProtos.MajorType.newBuilder().setMinorType(MinorType.VARBINARY).setMode(mode).build();
            } else if (convertedType == ConvertedType.DECIMAL) {
              return Types.withScaleAndPrecision(getDecimalType(schemaElement), DataMode.REPEATED, schemaElement.getScale(), schemaElement.getPrecision());
            }
          default:
            throw new UnsupportedOperationException("Type not supported: " + primitiveTypeName);
        }
    }
    throw new UnsupportedOperationException("Type not supported: " + primitiveTypeName + " Mode: " + mode);
  }
}
