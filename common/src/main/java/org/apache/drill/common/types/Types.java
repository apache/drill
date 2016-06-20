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

  public static boolean isUnion(MajorType toType) {
    return toType.getMinorType() == MinorType.UNION;
  }

  public static enum Comparability {
    UNKNOWN, NONE, EQUAL, ORDERED;
  }

  public static boolean isComplex(final MajorType type) {
    switch(type.getMinorType()) {
    case LIST:
    case MAP:
      return true;
    }

    return false;
  }

  public static boolean isRepeated(final MajorType type) {
    return type.getMode() == REPEATED ;
  }

  public static boolean isNumericType(final MajorType type) {
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

  /***
   * Gets SQL data type name for given Drill RPC-/protobuf-level data type.
   * @return
   *   canonical keyword sequence for SQL data type (leading keywords in
   *   corresponding {@code <data type>}; what
   *   {@code INFORMATION_SCHEMA.COLUMNS.TYPE_NAME} would list)
   */
  public static String getSqlTypeName(final MajorType type) {
    if (type.getMode() == DataMode.REPEATED || type.getMinorType() == MinorType.LIST) {
      return "ARRAY";
    }

    switch (type.getMinorType()) {

      // Standard SQL atomic data types:

      case BIT:             return "BOOLEAN";

      case SMALLINT:        return "SMALLINT";
      case INT:             return "INTEGER";
      case BIGINT:          return "BIGINT";

      case FLOAT4:          return "FLOAT";
      case FLOAT8:          return "DOUBLE";

      case DECIMAL9:
      case DECIMAL18:
      case DECIMAL28DENSE:
      case DECIMAL28SPARSE:
      case DECIMAL38DENSE:
      case DECIMAL38SPARSE: return "DECIMAL";

      case VARCHAR:         return "CHARACTER VARYING";
      case FIXEDCHAR:       return "CHARACTER";

      case VAR16CHAR:       return "NATIONAL CHARACTER VARYING";
      case FIXED16CHAR:     return "NATIONAL CHARACTER";

      case VARBINARY:       return "BINARY VARYING";
      case FIXEDBINARY:     return "BINARY";

      case DATE:            return "DATE";
      case TIME:            return "TIME";
      case TIMETZ:          return "TIME WITH TIME ZONE";
      case TIMESTAMP:       return "TIMESTAMP";
      case TIMESTAMPTZ:     return "TIMESTAMP WITH TIME ZONE";

      case INTERVALYEAR:
      case INTERVALDAY:     return "INTERVAL";

      // Non-standard SQL atomic data types:

      case INTERVAL:        return "INTERVAL";
      case MONEY:           return "DECIMAL";
      case TINYINT:         return "TINYINT";

      // Composite types and other types that are not atomic types (SQL standard
      // or not) except ARRAY types (handled above):

      case MAP:             return "MAP";
      case LATE:            return "ANY";
      case NULL:            return "NULL";
      case UNION:           return "UNION";

      // Internal types not actually used at level of SQL types(?):

      case UINT1:          return "TINYINT";
      case UINT2:          return "SMALLINT";
      case UINT4:          return "INTEGER";
      case UINT8:          return "BIGINT";

      default:
        throw new AssertionError(
            "Unexpected/unhandled MinorType value " + type.getMinorType() );
    }
  }

  /***
   * Gets JDBC type code for given SQL data type name.
   */
  public static int getJdbcTypeCode(final String sqlTypeName) {

    switch (sqlTypeName) {
      case "ANY":                           return java.sql.Types.OTHER;
      case "ARRAY":                         return java.sql.Types.ARRAY;
      case "BIGINT":                        return java.sql.Types.BIGINT;
      case "BINARY VARYING":                return java.sql.Types.VARBINARY;
      case "BINARY":                        return java.sql.Types.BINARY;
      case "BOOLEAN":                       return java.sql.Types.BOOLEAN;
      case "CHARACTER VARYING":             return java.sql.Types.VARCHAR;
      case "CHARACTER":                     return java.sql.Types.NCHAR;
      case "DATE":                          return java.sql.Types.DATE;
      case "DECIMAL":                       return java.sql.Types.DECIMAL;
      case "DOUBLE":                        return java.sql.Types.DOUBLE;
      case "FLOAT":                         return java.sql.Types.FLOAT;
      case "INTEGER":                       return java.sql.Types.INTEGER;
      case "INTERVAL":                      return java.sql.Types.OTHER;  // JDBC (4.1) has nothing for INTERVAL
      case "MAP":                           return java.sql.Types.STRUCT;
      case "NATIONAL CHARACTER VARYING":    return java.sql.Types.NVARCHAR;
      case "NATIONAL CHARACTER":            return java.sql.Types.NCHAR;
      case "NULL":                          return java.sql.Types.NULL;
      case "SMALLINT":                      return java.sql.Types.SMALLINT;
      case "TIME WITH TIME ZONE":           // fall through
      case "TIME":                          return java.sql.Types.TIME;
      case "TIMESTAMP WITH TIME ZONE":      // fall through
      case "TIMESTAMP":                     return java.sql.Types.TIMESTAMP;
      case "TINYINT":                       return java.sql.Types.TINYINT;
      case "UNION":                         return java.sql.Types.OTHER;
      default:
        // TODO:  This isn't really an unsupported-operation/-type case; this
        //   is an unexpected, code-out-of-sync-with-itself case, so use an
        //   exception intended for that.
        throw new UnsupportedOperationException(
            "Unexpected/unhandled SqlType value " + sqlTypeName );
    }
  }

  /**
   * Reports whether given RPC-level type is a signed type (per semantics of
   * {@link ResultSetMetaData#isSigned(int)}).
   */
  public static boolean isJdbcSignedType( final MajorType type ) {
    final boolean isSigned;
    switch ( type.getMode() ) {
      case REPEATED:
        isSigned = false;   // SQL ARRAY
        break;
      case REQUIRED:
      case OPTIONAL:
        switch ( type.getMinorType() ) {
          // Verified signed types:
          case SMALLINT:
          case INT:             // SQL INTEGER
          case BIGINT:
          case FLOAT4:          // SQL REAL / FLOAT(N)
          case FLOAT8:          // SQL DOUBLE PRECISION / FLOAT(N)
          case INTERVALYEAR:    // SQL INTERVAL w/YEAR and/or MONTH
          case INTERVALDAY:     // SQL INTERVAL w/DAY, HOUR, MINUTE and/or SECOND
          // Not-yet seen/verified signed types:
          case DECIMAL9:        // SQL DECIMAL (if used)
          case DECIMAL18:       // SQL DECIMAL (if used)
          case DECIMAL28SPARSE: // SQL DECIMAL (if used)
          case DECIMAL38SPARSE: // SQL DECIMAL (if used)
          case DECIMAL28DENSE:  // SQL DECIMAL (if used)
          case DECIMAL38DENSE:  // SQL DECIMAL (if used)
          case TINYINT:         // (not standard SQL)
          case MONEY:           // (not standard SQL)
          case INTERVAL:        // unknown (given INTERVALYEAR and INTERVALDAY)
            isSigned = true;
            break;
          // Verified unsigned types:
          case BIT:            // SQL BOOLEAN
          case VARCHAR:
          case FIXEDCHAR:      // SQL CHARACTER
          case VARBINARY:
          case FIXEDBINARY:    // SQL BINARY
          case DATE:
          case TIME:           // SQL TIME WITHOUT TIME ZONE
          case TIMESTAMP:      // SQL TIMESTAMP WITHOUT TIME ZONE
          // Not-yet seen/verified unsigned types:
          case UINT1:
          case UINT2:
          case UINT4:
          case UINT8:
          case FIXED16CHAR:
          case VAR16CHAR:
          case GENERIC_OBJECT:
          case LATE:
          case LIST:
          case MAP:
          case UNION:
          case NULL:
          case TIMETZ:      // SQL TIME WITH TIME ZONE
          case TIMESTAMPTZ: // SQL TIMESTAMP WITH TIME ZONE
            isSigned = false;
            break;
          default:
            throw new UnsupportedOperationException(
                "Unexpected/unhandled MinorType value " + type.getMinorType() );
        }
        break;
      default:
        throw new UnsupportedOperationException(
            "Unexpected/unhandled DataMode value " + type.getMode() );
    }
    return isSigned;
  }

  public static boolean usesHolderForGet(final MajorType type) {
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

  public static boolean isFixedWidthType(final MajorType type) {
    switch(type.getMinorType()) {
    case VARBINARY:
    case VAR16CHAR:
    case VARCHAR:
    case UNION:
      return false;
    default:
      return true;
    }
  }


  public static boolean isStringScalarType(final MajorType type) {
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

  public static boolean isBytesScalarType(final MajorType type) {
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

  public static Comparability getComparability(final MajorType type) {
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


  public static boolean softEquals(final MajorType a, final MajorType b, final boolean allowNullSwap) {
    if (a.getMinorType() != b.getMinorType()) {
        return false;
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

  public static boolean isLateBind(final MajorType type) {
    return type.getMinorType() == MinorType.LATE;
  }

  public static MajorType withMode(final MinorType type, final DataMode mode) {
    return MajorType.newBuilder().setMode(mode).setMinorType(type).build();
  }

  public static MajorType withScaleAndPrecision(final MinorType type, final DataMode mode, final int scale, final int precision) {
    return MajorType.newBuilder().setMinorType(type).setMode(mode).setScale(scale).setPrecision(precision).build();
  }

  public static MajorType required(final MinorType type) {
    return MajorType.newBuilder().setMode(DataMode.REQUIRED).setMinorType(type).build();
  }

  public static MajorType repeated(final MinorType type) {
    return MajorType.newBuilder().setMode(REPEATED).setMinorType(type).build();
  }

  public static MajorType optional(final MinorType type) {
    return MajorType.newBuilder().setMode(DataMode.OPTIONAL).setMinorType(type).build();
  }

  public static MajorType overrideMinorType(final MajorType originalMajorType, final MinorType overrideMinorType) {
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

  public static MajorType overrideMode(final MajorType originalMajorType, final DataMode overrideMode) {
    return withScaleAndPrecision(originalMajorType.getMinorType(), overrideMode, originalMajorType.getScale(), originalMajorType.getPrecision());
  }

  public static MajorType getMajorTypeFromName(final String typeName) {
    return getMajorTypeFromName(typeName, DataMode.REQUIRED);
  }

  public static MinorType getMinorTypeFromName(String typeName) {
    typeName = typeName.toLowerCase();

    switch (typeName) {
    case "bool":
    case "boolean":
      return MinorType.BIT;
    case "tinyint":
      return MinorType.TINYINT;
    case "uint1":
      return MinorType.UINT1;
    case "smallint":
      return MinorType.SMALLINT;
    case "uint2":
      return MinorType.UINT2;
    case "integer":
    case "int":
      return MinorType.INT;
    case "uint4":
      return MinorType.UINT4;
    case "bigint":
      return MinorType.BIGINT;
    case "uint8":
      return MinorType.UINT8;
    case "float":
      return MinorType.FLOAT4;
    case "double":
      return MinorType.FLOAT8;
    case "decimal":
      return MinorType.DECIMAL38SPARSE;
    case "symbol":
    case "char":
    case "utf8":
    case "varchar":
      return MinorType.VARCHAR;
    case "utf16":
    case "string":
    case "var16char":
      return MinorType.VAR16CHAR;
    case "timestamp":
      return MinorType.TIMESTAMP;
    case "interval_year_month":
      return MinorType.INTERVALYEAR;
    case "interval_day_time":
      return MinorType.INTERVALDAY;
    case "date":
      return MinorType.DATE;
    case "time":
      return MinorType.TIME;
    case "binary":
      return MinorType.VARBINARY;
    case "json":
    case "simplejson":
    case "extendedjson":
      return MinorType.LATE;
    case "null":
    case "any":
      return MinorType.NULL;
    default:
      throw new UnsupportedOperationException("Could not determine type: " + typeName);
    }
  }

  public static MajorType getMajorTypeFromName(final String typeName, final DataMode mode) {
    return withMode(getMinorTypeFromName(typeName), mode);
  }

  public static String getNameOfMinorType(final MinorType type) {
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

  public static String toString(final MajorType type) {
    return type != null ? "MajorType[" + TextFormat.shortDebugString(type) + "]" : "null";
  }

  /**
   * Get the <code>precision</code> of given type.
   * @param majorType
   * @return
   */
  public static int getPrecision(MajorType majorType) {
    MinorType type = majorType.getMinorType();

    if (type == MinorType.VARBINARY || type == MinorType.VARCHAR) {
      return 65536;
    }

    if (majorType.hasPrecision()) {
      return majorType.getPrecision();
    }

    return 0;
  }

  /**
   * Get the <code>scale</code> of given type.
   * @param majorType
   * @return
   */
  public static int getScale(MajorType majorType) {
    if (majorType.hasScale()) {
      return majorType.getScale();
    }

    return 0;
  }

  /**
   * Is the given type column be used in ORDER BY clause?
   * @param type
   * @return
   */
  public static boolean isSortable(MinorType type) {
    // Currently only map and list columns are not sortable.
    return type != MinorType.MAP && type != MinorType.LIST;
  }
}
