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
package org.apache.drill.common.expression.fn;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;

public class CastFunctions {

  private static Map<MinorType, String> TYPE2FUNC = new HashMap<>();
  /** The cast functions that need to be replaced (if
   * "drill.exec.functions.cast_empty_string_to_null" is set to true). */
  private static Set<String> CAST_FUNC_REPLACEMENT_NEEDED = new HashSet<>();
  /** Map from the replaced functions to the new ones (for non-nullable VARCHAR). */
  private static Map<String, String> CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR = new HashMap<>();
  /** Map from the replaced functions to the new ones (for non-nullable VAR16CHAR). */
  private static Map<String, String> CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR = new HashMap<>();
  /** Map from the replaced functions to the new ones (for non-nullable VARBINARY). */
  private static Map<String, String> CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY = new HashMap<>();
  /** Map from the replaced functions to the new ones (for nullable VARCHAR). */
  private static Map<String, String> CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR = new HashMap<>();
  /** Map from the replaced functions to the new ones (for nullable VAR16CHAR). */
  private static Map<String, String> CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR = new HashMap<>();
  /** Map from the replaced functions to the new ones (for nullable VARBINARY). */
  private static Map<String, String> CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY = new HashMap<>();
  static {
    TYPE2FUNC.put(MinorType.UNION, "castUNION");
    TYPE2FUNC.put(MinorType.BIGINT, "castBIGINT");
    TYPE2FUNC.put(MinorType.INT, "castINT");
    TYPE2FUNC.put(MinorType.BIT, "castBIT");
    TYPE2FUNC.put(MinorType.TINYINT, "castTINYINT");
    TYPE2FUNC.put(MinorType.FLOAT4, "castFLOAT4");
    TYPE2FUNC.put(MinorType.FLOAT8, "castFLOAT8");
    TYPE2FUNC.put(MinorType.VARCHAR, "castVARCHAR");
    TYPE2FUNC.put(MinorType.VAR16CHAR, "castVAR16CHAR");
    TYPE2FUNC.put(MinorType.VARBINARY, "castVARBINARY");
    TYPE2FUNC.put(MinorType.DATE, "castDATE");
    TYPE2FUNC.put(MinorType.TIME, "castTIME");
    TYPE2FUNC.put(MinorType.TIMESTAMP, "castTIMESTAMP");
    TYPE2FUNC.put(MinorType.TIMESTAMPTZ, "castTIMESTAMPTZ");
    TYPE2FUNC.put(MinorType.INTERVALDAY, "castINTERVALDAY");
    TYPE2FUNC.put(MinorType.INTERVALYEAR, "castINTERVALYEAR");
    TYPE2FUNC.put(MinorType.INTERVAL, "castINTERVAL");
    TYPE2FUNC.put(MinorType.DECIMAL9, "castDECIMAL9");
    TYPE2FUNC.put(MinorType.DECIMAL18, "castDECIMAL18");
    TYPE2FUNC.put(MinorType.DECIMAL28SPARSE, "castDECIMAL28SPARSE");
    TYPE2FUNC.put(MinorType.DECIMAL28DENSE, "castDECIMAL28DENSE");
    TYPE2FUNC.put(MinorType.DECIMAL38SPARSE, "castDECIMAL38SPARSE");
    TYPE2FUNC.put(MinorType.DECIMAL38DENSE, "castDECIMAL38DENSE");
    TYPE2FUNC.put(MinorType.VARDECIMAL, "castVARDECIMAL");

    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.INT));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.BIGINT));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.FLOAT4));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.FLOAT8));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.DECIMAL9));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.DECIMAL18));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.DECIMAL28SPARSE));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.DECIMAL38SPARSE));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.VARDECIMAL));

    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.INT), "castEmptyStringVarCharToNullableINT");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.BIGINT), "castEmptyStringVarCharToNullableBIGINT");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.FLOAT4), "castEmptyStringVarCharToNullableFLOAT4");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.FLOAT8), "castEmptyStringVarCharToNullableFLOAT8");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.DECIMAL9), "castEmptyStringVarCharToNullableDECIMAL9");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.DECIMAL18), "castEmptyStringVarCharToNullableDECIMAL18");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.DECIMAL28SPARSE), "castEmptyStringVarCharToNullableDECIMAL28SPARSE");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.DECIMAL38SPARSE), "castEmptyStringVarCharToNullableDECIMAL38SPARSE");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.VARDECIMAL), "castEmptyStringVarCharToNullableVARDECIMAL");

    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.INT), "castEmptyStringVar16CharToNullableINT");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.BIGINT), "castEmptyStringVar16CharToNullableBIGINT");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.FLOAT4), "castEmptyStringVar16CharToNullableFLOAT4");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.FLOAT8), "castEmptyStringVar16CharToNullableFLOAT8");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.DECIMAL9), "castEmptyStringVar16CharToNullableDECIMAL9");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.DECIMAL18), "castEmptyStringVar16CharToNullableDECIMAL18");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.DECIMAL28SPARSE), "castEmptyStringVar16CharToNullableDECIMAL28SPARSE");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.DECIMAL38SPARSE), "castEmptyStringVar16CharToNullableDECIMAL38SPARSE");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.VARDECIMAL), "castEmptyStringVar16CharToNullableVARDECIMAL");

    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.INT), "castEmptyStringVarBinaryToNullableINT");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.BIGINT), "castEmptyStringVarBinaryToNullableBIGINT");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.FLOAT4), "castEmptyStringVarBinaryToNullableFLOAT4");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.FLOAT8), "castEmptyStringVarBinaryToNullableFLOAT8");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.DECIMAL9), "castEmptyStringVarBinaryToNullableDECIMAL9");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.DECIMAL18), "castEmptyStringVarBinaryToNullableDECIMAL18");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.DECIMAL28SPARSE), "castEmptyStringVarBinaryToNullableDECIMAL28SPARSE");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.DECIMAL38SPARSE), "castEmptyStringVarBinaryToNullableDECIMAL38SPARSE");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.VARDECIMAL), "castEmptyStringVarBinaryToNullableVARDECIMAL");

    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.INT), "castEmptyStringNullableVarCharToNullableINT");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.BIGINT), "castEmptyStringNullableVarCharToNullableBIGINT");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.FLOAT4), "castEmptyStringNullableVarCharToNullableFLOAT4");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.FLOAT8), "castEmptyStringNullableVarCharToNullableFLOAT8");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.DECIMAL9), "castEmptyStringNullableVarCharToNullableDECIMAL9");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.DECIMAL18), "castEmptyStringNullableVarCharToNullableDECIMAL18");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.DECIMAL28SPARSE), "castEmptyStringNullableVarCharToNullableDECIMAL28SPARSE");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.DECIMAL38SPARSE), "castEmptyStringNullableVarCharToNullableDECIMAL38SPARSE");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.put(TYPE2FUNC.get(MinorType.VARDECIMAL), "castEmptyStringNullableVarCharToNullableVARDECIMAL");

    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.INT), "castEmptyStringNullableVar16CharToNullableINT");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.BIGINT), "castEmptyStringNullableVar16CharToNullableBIGINT");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.FLOAT4), "castEmptyStringNullableVar16CharToNullableFLOAT4");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.FLOAT8), "castEmptyStringNullableVar16CharToNullableFLOAT8");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.DECIMAL9), "castEmptyStringNullableVar16CharToNullableDECIMAL9");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.DECIMAL18), "castEmptyStringNullableVar16CharToNullableDECIMAL18");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.DECIMAL28SPARSE), "castEmptyStringNullableVar16CharToNullableDECIMAL28SPARSE");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.DECIMAL38SPARSE), "castEmptyStringNullableVar16CharToNullableDECIMAL38SPARSE");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.put(TYPE2FUNC.get(MinorType.VARDECIMAL), "castEmptyStringNullableVar16CharToNullableVARDECIMAL");

    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.INT), "castEmptyStringNullableVarBinaryToNullableINT");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.BIGINT), "castEmptyStringNullableVarBinaryToNullableBIGINT");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.FLOAT4), "castEmptyStringNullableVarBinaryToNullableFLOAT4");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.FLOAT8), "castEmptyStringNullableVarBinaryToNullableFLOAT8");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.DECIMAL9), "castEmptyStringNullableVarBinaryToNullableDECIMAL9");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.DECIMAL18), "castEmptyStringNullableVarBinaryToNullableDECIMAL18");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.DECIMAL28SPARSE), "castEmptyStringNullableVarBinaryToNullableDECIMAL28SPARSE");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.DECIMAL38SPARSE), "castEmptyStringNullableVarBinaryToNullableDECIMAL38SPARSE");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.put(TYPE2FUNC.get(MinorType.VARDECIMAL), "castEmptyStringNullableVarBinaryToNullableVARDECIMAL");
  }

  /**
  * Given the target type, get the appropriate cast function
  * @param targetMinorType the target data type
  * @return the name of cast function
  */
  public static String getCastFunc(MinorType targetMinorType) {
    String func = TYPE2FUNC.get(targetMinorType);
    if (func != null) {
      return func;
    }

    throw new IllegalArgumentException(
      String.format("cast function for type %s is not defined", targetMinorType.name()));
  }

  /**
  * Get a replacing cast function for the original function, based on the specified data mode
  * @param originalCastFunction original cast function
  * @param dataMode data mode of the input data
  * @param inputType input (minor) type for cast
  * @return the name of replaced cast function
  */
  public static String getReplacingCastFunction(String originalCastFunction, DataMode dataMode, MinorType inputType) {
    if(dataMode == DataMode.OPTIONAL) {
      return getReplacingCastFunctionFromNullable(originalCastFunction, inputType);
    }

    if(dataMode == DataMode.REQUIRED) {
      return getReplacingCastFunctionFromNonNullable(originalCastFunction, inputType);
    }

    throw new RuntimeException(
       String.format("replacing cast function for datatype %s is not defined", dataMode));
  }

  /**
  * Check if a replacing cast function is available for the the original function
  * @param originalfunction original cast function
  * @param inputType input (minor) type for cast
  * @return true if replacement is needed, false - if isn't
  */
  public static boolean isReplacementNeeded(String originalfunction, MinorType inputType) {
    return (inputType == MinorType.VARCHAR || inputType == MinorType.VARBINARY || inputType == MinorType.VAR16CHAR) &&
        CAST_FUNC_REPLACEMENT_NEEDED.contains(originalfunction);
  }

  /**
   * Check if a funcName is one of the cast function.
   * @param funcName
   * @return
   */
  public static boolean isCastFunction(String funcName) {
    return TYPE2FUNC.values().contains(funcName);
  }

  private static String getReplacingCastFunctionFromNonNullable(String originalCastFunction, MinorType inputType) {
    if(inputType == MinorType.VARCHAR && CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR.containsKey(originalCastFunction)) {
      return CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR.get(originalCastFunction);
    }
    if(inputType == MinorType.VAR16CHAR && CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR.containsKey(originalCastFunction)) {
      return CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR.get(originalCastFunction);
    }
    if(inputType == MinorType.VARBINARY && CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY.containsKey(originalCastFunction)) {
      return CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY.get(originalCastFunction);
    }

    throw new RuntimeException(
      String.format("replacing cast function for %s is not defined", originalCastFunction));
  }

  private static String getReplacingCastFunctionFromNullable(String originalCastFunction, MinorType inputType) {
    if(inputType == MinorType.VARCHAR && CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.containsKey(originalCastFunction)) {
      return CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.get(originalCastFunction);
    }
    if(inputType == MinorType.VAR16CHAR && CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.containsKey(originalCastFunction)) {
      return CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.get(originalCastFunction);
    }
    if(inputType == MinorType.VARBINARY && CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.containsKey(originalCastFunction)) {
      return CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.get(originalCastFunction);
    }

    throw new RuntimeException(
      String.format("replacing cast function for %s is not defined", originalCastFunction));
  }
}
