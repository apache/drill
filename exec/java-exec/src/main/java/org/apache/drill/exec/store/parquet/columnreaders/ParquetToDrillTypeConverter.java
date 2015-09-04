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

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MinorType;

import org.apache.drill.common.util.CoreDecimalUtility;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.parquet.ParquetReaderUtility;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.schema.PrimitiveType;

import static com.google.common.base.Preconditions.checkArgument;

public class ParquetToDrillTypeConverter {

  private static TypeProtos.MinorType getDecimalType(SchemaElement schemaElement) {
    return schemaElement.getPrecision() <= 28 ? TypeProtos.MinorType.DECIMAL28SPARSE : MinorType.DECIMAL38SPARSE;
  }

  private static TypeProtos.MinorType getMinorType(PrimitiveType.PrimitiveTypeName primitiveTypeName, int length,
                                                   SchemaElement schemaElement, OptionManager options) {

    ConvertedType convertedType = schemaElement.getConverted_type();

    switch (primitiveTypeName) {
      case BINARY:
        if (convertedType == null) {
          return (TypeProtos.MinorType.VARBINARY);
        }
        switch (convertedType) {
          case UTF8:
            return (TypeProtos.MinorType.VARCHAR);
          case DECIMAL:
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
            return (getDecimalType(schemaElement));
          default:
            return (TypeProtos.MinorType.VARBINARY);
        }
      case INT64:
        if (convertedType == null) {
          return (TypeProtos.MinorType.BIGINT);
        }
        switch(convertedType) {
          case DECIMAL:
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
            return TypeProtos.MinorType.DECIMAL18;
          // TODO - add this back if it is decided to be added upstream, was removed form our pull request July 2014
//              case TIME_MICROS:
//                throw new UnsupportedOperationException();
          case TIMESTAMP_MILLIS:
            return TypeProtos.MinorType.TIMESTAMP;
          default:
            throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, convertedType));
        }
      case INT32:
        if (convertedType == null) {
          return TypeProtos.MinorType.INT;
        }
        switch(convertedType) {
          case DECIMAL:
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
            return TypeProtos.MinorType.DECIMAL9;
          case DATE:
            return TypeProtos.MinorType.DATE;
          case TIME_MILLIS:
            return TypeProtos.MinorType.TIME;
          default:
            throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, convertedType));
        }
      case BOOLEAN:
        return TypeProtos.MinorType.BIT;
      case FLOAT:
        return TypeProtos.MinorType.FLOAT4;
      case DOUBLE:
        return TypeProtos.MinorType.FLOAT8;
      // TODO - Both of these are not supported by the parquet library yet (7/3/13),
      // but they are declared here for when they are implemented
      case INT96:
        return TypeProtos.MinorType.VARBINARY;
      case FIXED_LEN_BYTE_ARRAY:
        if (convertedType == null) {
          checkArgument(length > 0, "A length greater than zero must be provided for a FixedBinary type.");
          return TypeProtos.MinorType.VARBINARY;
        } else if (convertedType == ConvertedType.DECIMAL) {
          ParquetReaderUtility.checkDecimalTypeEnabled(options);
          return getDecimalType(schemaElement);
        } else if (convertedType == ConvertedType.INTERVAL) {
          return TypeProtos.MinorType.INTERVAL;
        }
      default:
        throw new UnsupportedOperationException("Type not supported: " + primitiveTypeName);
    }
  }

  public static TypeProtos.MajorType toMajorType(PrimitiveType.PrimitiveTypeName primitiveTypeName, int length,
                                          TypeProtos.DataMode mode, SchemaElement schemaElement,
                                          OptionManager options) {
    MinorType minorType = getMinorType(primitiveTypeName, length, schemaElement, options);
    TypeProtos.MajorType.Builder typeBuilder = TypeProtos.MajorType.newBuilder().setMinorType(minorType).setMode(mode);

    if (CoreDecimalUtility.isDecimalType(minorType)) {
      typeBuilder.setPrecision(schemaElement.getPrecision()).setScale(schemaElement.getScale());
    }
    return typeBuilder.build();
  }
}
