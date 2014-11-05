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
package org.apache.drill.common.types;

import static org.apache.drill.common.types.TypeProtos.DataMode.REPEATED;
import static org.apache.drill.common.types.TypeProtos.MinorType.*;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;

import com.google.protobuf.TextFormat;

public class Types {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Types.class);

  public static final MajorType NULL = required(MinorType.NULL);
  public static final MajorType LATE_BIND_TYPE = optional(MinorType.LATE);
  public static final MajorType REQUIRED_BIT = required(MinorType.BIT);
  public static final MajorType OPTIONAL_BIT = optional(MinorType.BIT);

  public static enum Comparability {
    UNKNOWN, NONE, EQUAL, ORDERED;
  }

  public static boolean isComplex(MajorType type) {
    switch(type.getMinorType()) {
    case LIST:
    case MAP:
      return true;
    }

    return false;
  }

  public static boolean isRepeated(MajorType type) {
    return type.getMode() == REPEATED ;
  }

  public static boolean isNumericType(MajorType type) {
    if (type.getMode() == REPEATED) {
      return false;
    }

    switch(type.getMinorType()) {
    case BIGINT:
    case DECIMAL38SPARSE:
    case DECIMAL38DENSE:
    case DECIMAL28SPARSE:
    case DECIMAL28DENSE:
    case DECIMAL18:
    case DECIMAL9:
    case FLOAT4:
    case FLOAT8:
    case INT:
    case MONEY:
    case SMALLINT:
    case TINYINT:
    case UINT1:
    case UINT2:
    case UINT4:
    case UINT8:
      return true;
      default:
        return false;
    }
  }

  public static int getSqlType(MajorType type) {
    if (type.getMode() == DataMode.REPEATED) {
      return java.sql.Types.ARRAY;
    }

    switch(type.getMinorType()) {
    case BIGINT:
      return java.sql.Types.BIGINT;
    case BIT:
      return java.sql.Types.BOOLEAN;
    case DATE:
      return java.sql.Types.DATE;
    case DECIMAL9:
    case DECIMAL18:
    case DECIMAL28DENSE:
    case DECIMAL28SPARSE:
    case DECIMAL38DENSE:
    case DECIMAL38SPARSE:
      return java.sql.Types.DECIMAL;
    case FIXED16CHAR:
      return java.sql.Types.NCHAR;
    case FIXEDBINARY:
      return java.sql.Types.BINARY;
    case FIXEDCHAR:
      return java.sql.Types.NCHAR;
    case FLOAT4:
      return java.sql.Types.FLOAT;
    case FLOAT8:
      return java.sql.Types.DOUBLE;
    case INT:
      return java.sql.Types.INTEGER;
    case MAP:
      return java.sql.Types.STRUCT;
    case MONEY:
      return java.sql.Types.DECIMAL;
    case NULL:
    case INTERVAL:
    case INTERVALYEAR:
    case INTERVALDAY:
    case LATE:
    case SMALLINT:
      return java.sql.Types.SMALLINT;
    case TIME:
      return java.sql.Types.TIME;
    case TIMESTAMPTZ:
    case TIMESTAMP:
      return java.sql.Types.TIMESTAMP;
    case TIMETZ:
      return java.sql.Types.DATE;
    case TINYINT:
      return java.sql.Types.TINYINT;
    case UINT1:
      return java.sql.Types.TINYINT;
    case UINT2:
      return java.sql.Types.SMALLINT;
    case UINT4:
      return java.sql.Types.INTEGER;
    case UINT8:
      return java.sql.Types.BIGINT;
    case VAR16CHAR:
      return java.sql.Types.NVARCHAR;
    case VARBINARY:
      return java.sql.Types.VARBINARY;
    case VARCHAR:
      return java.sql.Types.NVARCHAR;
    default:
      throw new UnsupportedOperationException();
    }
  }

  public static boolean isUnSigned(MajorType type) {
    switch(type.getMinorType()) {
    case UINT1:
    case UINT2:
    case UINT4:
    case UINT8:
      return true;
    default:
      return false;
    }

  }
  public static boolean usesHolderForGet(MajorType type) {
    if (type.getMode() == REPEATED) {
      return true;
    }
    switch(type.getMinorType()) {
    case BIGINT:
    case FLOAT4:
    case FLOAT8:
    case INT:
    case MONEY:
    case SMALLINT:
    case TINYINT:
    case UINT1:
    case UINT2:
    case UINT4:
    case UINT8:
    case INTERVALYEAR:
    case DATE:
    case TIME:
    case TIMESTAMP:
      return false;

    default:
      return true;
    }

  }

  public static boolean isFixedWidthType(MajorType type) {
    switch(type.getMinorType()) {
    case VARBINARY:
    case VAR16CHAR:
    case VARCHAR:
      return false;
    default:
      return true;
    }
  }


  public static boolean isStringScalarType(MajorType type) {
    if (type.getMode() == REPEATED) {
      return false;
    }
    switch(type.getMinorType()) {
    case FIXEDCHAR:
    case FIXED16CHAR:
    case VARCHAR:
    case VAR16CHAR:
      return true;
    default:
      return false;
    }
  }

  public static boolean isBytesScalarType(MajorType type) {
    if (type.getMode() == REPEATED) {
      return false;
    }
    switch(type.getMinorType()) {
    case FIXEDBINARY:
    case VARBINARY:
      return true;
    default:
      return false;
    }
  }

  public static Comparability getComparability(MajorType type) {
    if (type.getMode() == REPEATED) {
      return Comparability.NONE;
    }
    if (type.getMinorType() == MinorType.LATE) {
      return Comparability.UNKNOWN;
    }

    switch(type.getMinorType()) {
    case LATE:
      return Comparability.UNKNOWN;
    case MAP:
      return Comparability.NONE;
    case BIT:
      return Comparability.EQUAL;
    default:
      return Comparability.ORDERED;
    }

  }


  public static boolean softEquals(MajorType a, MajorType b, boolean allowNullSwap) {
    if (a.getMinorType() != b.getMinorType()) {
      if (
          (a.getMinorType() == MinorType.VARBINARY && b.getMinorType() == MinorType.VARCHAR) ||
          (b.getMinorType() == MinorType.VARBINARY && a.getMinorType() == MinorType.VARCHAR)
          ) {
        // fall through;
      } else {
        return false;
      }

    }
    if(allowNullSwap) {
      switch (a.getMode()) {
      case OPTIONAL:
      case REQUIRED:
        switch (b.getMode()) {
        case OPTIONAL:
        case REQUIRED:
          return true;
        }
      }
    }
    return a.getMode() == b.getMode();
  }

  public static boolean isLateBind(MajorType type) {
    return type.getMinorType() == MinorType.LATE;
  }

  public static MajorType withMode(MinorType type, DataMode mode) {
    return MajorType.newBuilder().setMode(mode).setMinorType(type).build();
  }

  public static MajorType withScaleAndPrecision(MinorType type, DataMode mode, int scale, int precision) {
    return MajorType.newBuilder().setMinorType(type).setMode(mode).setScale(scale).setPrecision(precision).build();
  }

  public static MajorType required(MinorType type) {
    return MajorType.newBuilder().setMode(DataMode.REQUIRED).setMinorType(type).build();
  }

  public static MajorType repeated(MinorType type) {
    return MajorType.newBuilder().setMode(REPEATED).setMinorType(type).build();
  }

  public static MajorType optional(MinorType type) {
    return MajorType.newBuilder().setMode(DataMode.OPTIONAL).setMinorType(type).build();
  }

  public static MajorType overrideMinorType(MajorType originalMajorType, MinorType overrideMinorType) {
    switch (originalMajorType.getMode()) {
      case REPEATED:
        return repeated(overrideMinorType);
      case OPTIONAL:
        return optional(overrideMinorType);
      case REQUIRED:
        return required(overrideMinorType);
      default:
        throw new UnsupportedOperationException();
    }
  }

  public static MajorType overrideMode(MajorType originalMajorType, DataMode overrideMode) {
    return withScaleAndPrecision(originalMajorType.getMinorType(), overrideMode, originalMajorType.getScale(), originalMajorType.getPrecision());
  }

  public static MajorType getMajorTypeFromName(String typeName) {
    return getMajorTypeFromName(typeName, DataMode.REQUIRED);
  }

  public static MajorType getMajorTypeFromName(String typeName, DataMode mode) {
    switch (typeName) {
    case "bool":
    case "boolean":
      return withMode(MinorType.BIT, mode);
    case "tinyint":
      return withMode(MinorType.TINYINT, mode);
    case "uint1":
      return withMode(MinorType.UINT1, mode);
    case "smallint":
      return withMode(MinorType.SMALLINT, mode);
    case "uint2":
      return withMode(MinorType.UINT2, mode);
    case "int":
      return withMode(MinorType.INT, mode);
    case "uint4":
      return withMode(MinorType.UINT4, mode);
    case "bigint":
      return withMode(MinorType.BIGINT, mode);
    case "uint8":
      return withMode(MinorType.UINT8, mode);
    case "float":
      return withMode(MinorType.FLOAT4, mode);
    case "double":
      return withMode(MinorType.FLOAT8, mode);
    case "decimal":
      return withMode(MinorType.DECIMAL38SPARSE, mode);
    case "utf8":
    case "varchar":
      return withMode(MinorType.VARCHAR, mode);
    case "utf16":
    case "string":
    case "var16char":
      return withMode(MinorType.VAR16CHAR, mode);
    case "date":
      return withMode(MinorType.DATE, mode);
    case "time":
      return withMode(MinorType.TIME, mode);
    case "binary":
      return withMode(MinorType.VARBINARY, mode);
    case "json":
      return withMode(MinorType.LATE, mode);
    default:
      throw new UnsupportedOperationException("Could not determine type: " + typeName);
    }
  }

  public static String getNameOfMinorType(MinorType type) {
    switch (type) {
      case BIT:
        return "bool";
      case TINYINT:
        return "tinyint";
      case UINT1:
        return "uint1";
      case SMALLINT:
        return "smallint";
      case UINT2:
        return "uint2";
      case INT:
        return "int";
      case UINT4:
        return "uint4";
      case BIGINT:
        return "bigint";
      case UINT8:
        return "uint8";
      case FLOAT4:
        return "float";
      case FLOAT8:
        return "double";
      case DECIMAL9:
        return "decimal";
      case DECIMAL18:
        return "decimal";
      case DECIMAL28SPARSE:
        return "decimal";
      case DECIMAL38SPARSE:
        return "decimal";
      case VARCHAR:
        return "varchar";
      case VAR16CHAR:
        return "utf16";
      case DATE:
        return "date";
      case TIME:
        return "time";
      case TIMESTAMP:
        return "timestamp";
      case VARBINARY:
        return "binary";
      case LATE:
        throw new DrillRuntimeException("The late type should never appear in execution or an SQL query, so it does not have a name to refer to it.");
      default:
        throw new DrillRuntimeException("Unrecognized type " + type);
    }
  }

  public static String toString(MajorType type) {
    return type != null ? "MajorType[" + TextFormat.shortDebugString(type) + "]" : "null";
  }

}
