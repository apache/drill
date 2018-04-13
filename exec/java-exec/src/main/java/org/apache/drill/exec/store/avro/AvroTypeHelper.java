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
package org.apache.drill.exec.store.avro;

import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.Types;

/**
 * Utility class for working with Avro data types.
 */
public final class AvroTypeHelper {

  // XXX - Decide what to do about Avro's NULL type
  /*
  public static final MajorType MAJOR_TYPE_NULL_OPTIONAL      = Types.optional(MinorType.NULL);
  public static final MajorType MAJOR_TYPE_NULL_REQUIRED      = Types.required(MinorType.NULL);
  public static final MajorType MAJOR_TYPE_NULL_REPEATED      = Types.repeated(MinorType.NULL);
  */
  public static final MajorType MAJOR_TYPE_BOOL_OPTIONAL      = Types.optional(MinorType.UINT1);
  public static final MajorType MAJOR_TYPE_BOOL_REQUIRED      = Types.required(MinorType.UINT1);
  public static final MajorType MAJOR_TYPE_BOOL_REPEATED      = Types.repeated(MinorType.UINT1);
  public static final MajorType MAJOR_TYPE_INT_OPTIONAL       = Types.optional(MinorType.INT);
  public static final MajorType MAJOR_TYPE_INT_REQUIRED       = Types.required(MinorType.INT);
  public static final MajorType MAJOR_TYPE_INT_REPEATED       = Types.repeated(MinorType.INT);
  public static final MajorType MAJOR_TYPE_BIGINT_OPTIONAL    = Types.optional(MinorType.BIGINT);
  public static final MajorType MAJOR_TYPE_BIGINT_REQUIRED    = Types.required(MinorType.BIGINT);
  public static final MajorType MAJOR_TYPE_BIGINT_REPEATED    = Types.repeated(MinorType.BIGINT);
  public static final MajorType MAJOR_TYPE_FLOAT4_OPTIONAL    = Types.optional(MinorType.FLOAT4);
  public static final MajorType MAJOR_TYPE_FLOAT4_REQUIRED    = Types.required(MinorType.FLOAT4);
  public static final MajorType MAJOR_TYPE_FLOAT4_REPEATED    = Types.repeated(MinorType.FLOAT4);
  public static final MajorType MAJOR_TYPE_FLOAT8_OPTIONAL    = Types.optional(MinorType.FLOAT8);
  public static final MajorType MAJOR_TYPE_FLOAT8_REQUIRED    = Types.required(MinorType.FLOAT8);
  public static final MajorType MAJOR_TYPE_FLOAT8_REPEATED    = Types.repeated(MinorType.FLOAT8);
  public static final MajorType MAJOR_TYPE_VARBINARY_OPTIONAL = Types.optional(MinorType.VARBINARY);
  public static final MajorType MAJOR_TYPE_VARBINARY_REQUIRED = Types.required(MinorType.VARBINARY);
  public static final MajorType MAJOR_TYPE_VARBINARY_REPEATED = Types.repeated(MinorType.VARBINARY);
  public static final MajorType MAJOR_TYPE_VARCHAR_OPTIONAL   = Types.optional(MinorType.VARCHAR);
  public static final MajorType MAJOR_TYPE_VARCHAR_REQUIRED   = Types.required(MinorType.VARCHAR);
  public static final MajorType MAJOR_TYPE_VARCHAR_REPEATED   = Types.repeated(MinorType.VARCHAR);


  private static final String UNSUPPORTED = "Unsupported type: %s [%s]";

  private AvroTypeHelper() { }

  /**
   * Maintains a mapping between Avro types and Drill types. Given an Avro data
   * type, this method will return the corresponding Drill field major type.
   *
   * @param field   Avro field
   * @return        Major type or null if no corresponding type
   */
  public static MajorType getFieldMajorType(final Field field, final DataMode mode) {
    return getFieldMajorType(field.schema().getType(), mode);
  }

  /**
   * Maintains a mapping between Avro types and Drill types. Given an Avro data
   * type, this method will return the corresponding Drill field major type.
   *
   * @param type Avro type
   * @param mode Data mode
   * @return     Drill major type or null if no corresponding type
   */
  public static MajorType getFieldMajorType(final Type type, final DataMode mode) {

    switch (type) {
      case MAP:
      case RECORD:
      case ENUM:
      case UNION:
        throw new UnsupportedOperationException("Complex types are unimplemented");
      case NULL:
        /*
        switch (mode) {
          case OPTIONAL:
            return MAJOR_TYPE_NULL_OPTIONAL;
          case REQUIRED:
            return MAJOR_TYPE_NULL_REQUIRED;
          case REPEATED:
            return MAJOR_TYPE_NULL_REPEATED;
        }
        break;
        */
        throw new UnsupportedOperationException(String.format(UNSUPPORTED, type.getName(), mode.name()));
      case ARRAY:
        break;
      case BOOLEAN:
        switch (mode) {
          case OPTIONAL:
            return MAJOR_TYPE_BOOL_OPTIONAL;
          case REQUIRED:
            return MAJOR_TYPE_BOOL_REQUIRED;
          case REPEATED:
            return MAJOR_TYPE_BOOL_REPEATED;
        }
        break;
      case INT:
        switch (mode) {
          case OPTIONAL:
            return MAJOR_TYPE_INT_OPTIONAL;
          case REQUIRED:
            return MAJOR_TYPE_INT_REQUIRED;
          case REPEATED:
            return MAJOR_TYPE_INT_REPEATED;
        }
        break;
      case LONG:
        switch (mode) {
          case OPTIONAL:
            return MAJOR_TYPE_BIGINT_OPTIONAL;
          case REQUIRED:
            return MAJOR_TYPE_BIGINT_REQUIRED;
          case REPEATED:
            return MAJOR_TYPE_BIGINT_REPEATED;
        }
        break;
      case FLOAT:
        switch (mode) {
          case OPTIONAL:
            return MAJOR_TYPE_FLOAT4_OPTIONAL;
          case REQUIRED:
            return MAJOR_TYPE_FLOAT4_REQUIRED;
          case REPEATED:
            return MAJOR_TYPE_FLOAT4_REPEATED;
        }
        break;
      case DOUBLE:
        switch (mode) {
          case OPTIONAL:
            return MAJOR_TYPE_FLOAT8_OPTIONAL;
          case REQUIRED:
            return MAJOR_TYPE_FLOAT8_REQUIRED;
          case REPEATED:
            return MAJOR_TYPE_FLOAT8_REPEATED;
        }
        break;
      case BYTES:
        switch (mode) {
          case OPTIONAL:
            return MAJOR_TYPE_VARBINARY_OPTIONAL;
          case REQUIRED:
            return MAJOR_TYPE_VARBINARY_REQUIRED;
          case REPEATED:
            return MAJOR_TYPE_VARBINARY_REPEATED;
        }
        break;
      case STRING:
        switch (mode) {
          case OPTIONAL:
            return MAJOR_TYPE_VARCHAR_OPTIONAL;
          case REQUIRED:
            return MAJOR_TYPE_VARCHAR_REQUIRED;
          case REPEATED:
            return MAJOR_TYPE_VARCHAR_REPEATED;
        }
        break;
      default:
        throw new UnsupportedOperationException(String.format(UNSUPPORTED, type.getName(), mode.name()));
    }

    throw new UnsupportedOperationException(String.format(UNSUPPORTED, type.getName(), mode.name()));
  }
}
