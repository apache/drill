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
package org.apache.drill.common.expression.fn;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MinorType;

public class CastFunctions {

  private static Map<MinorType, String> TYPE2FUNC = new HashMap<>();
  /** The cast functions that need to be replaced (if
   * "drill.exec.functions.cast_empty_string_to_null" is set to true). */
  private static Set<String> CAST_FUNC_REPLACEMENT_NEEDED = new HashSet<>();
  /** Map from the replaced functions to the new ones (for non-nullable VARCHAR). */
  private static Map<String, String> CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE = new HashMap<>();
  /** Map from the replaced functions to the new ones (for nullable VARCHAR). */
  private static Map<String, String> CAST_FUNC_REPLACEMENT_FROM_NULLABLE = new HashMap<>();

  static {
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

    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.INT));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.BIGINT));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.FLOAT4));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.FLOAT8));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.DECIMAL9));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.DECIMAL18));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.DECIMAL28SPARSE));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.DECIMAL38SPARSE));

    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE.put(TYPE2FUNC.get(MinorType.INT), "castEmptyStringVarCharToNullableINT");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE.put(TYPE2FUNC.get(MinorType.BIGINT), "castEmptyStringVarCharToNullableBIGINT");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE.put(TYPE2FUNC.get(MinorType.FLOAT4), "castEmptyStringVarCharToNullableFLOAT4");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE.put(TYPE2FUNC.get(MinorType.FLOAT8), "castEmptyStringVarCharToNullableFLOAT8");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE.put(TYPE2FUNC.get(MinorType.DECIMAL9), "castEmptyStringVarCharToNullableDECIMAL9");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE.put(TYPE2FUNC.get(MinorType.DECIMAL18), "castEmptyStringVarCharToNullableDECIMAL18");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE.put(TYPE2FUNC.get(MinorType.DECIMAL28SPARSE), "castEmptyStringVarCharToNullableDECIMAL28SPARSE");
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE.put(TYPE2FUNC.get(MinorType.DECIMAL38SPARSE), "castEmptyStringVarCharToNullableDECIMAL38SPARSE");

    CAST_FUNC_REPLACEMENT_FROM_NULLABLE.put(TYPE2FUNC.get(MinorType.INT), "castEmptyStringNullableVarCharToNullableINT");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE.put(TYPE2FUNC.get(MinorType.BIGINT), "castEmptyStringNullableVarCharToNullableBIGINT");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE.put(TYPE2FUNC.get(MinorType.FLOAT4), "castEmptyStringNullableVarCharToNullableFLOAT4");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE.put(TYPE2FUNC.get(MinorType.FLOAT8), "castEmptyStringNullableVarCharToNullableFLOAT8");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE.put(TYPE2FUNC.get(MinorType.DECIMAL9), "castEmptyStringNullableVarCharToNullableDECIMAL9");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE.put(TYPE2FUNC.get(MinorType.DECIMAL18), "castEmptyStringNullableVarCharToNullableDECIMAL18");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE.put(TYPE2FUNC.get(MinorType.DECIMAL28SPARSE), "castEmptyStringNullableVarCharToNullableDECIMAL28SPARSE");
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE.put(TYPE2FUNC.get(MinorType.DECIMAL38SPARSE), "castEmptyStringNullableVarCharToNullableDECIMAL38SPARSE");
  }

  /**
  * Given the target type, get the appropriate cast function
  * @param targetMinorType the target data type
  * @return
  */
  public static String getCastFunc(MinorType targetMinorType) {
    String func = TYPE2FUNC.get(targetMinorType);
    if (func != null) {
      return func;
    }

    throw new RuntimeException(
      String.format("cast function for type %s is not defined", targetMinorType.name()));
  }

  /**
  * Get a replacing cast function for the original function, based on the specified data mode
  * @param originalCastFunction original cast function
  * @param dataMode data mode of the input data
  * @return
  */
  public static String getReplacingCastFunction(String originalCastFunction, org.apache.drill.common.types.TypeProtos.DataMode dataMode) {
    if(dataMode == TypeProtos.DataMode.OPTIONAL) {
      return getReplacingCastFunctionFromNullable(originalCastFunction);
    }

    if(dataMode == TypeProtos.DataMode.REQUIRED) {
      return getReplacingCastFunctionFromNonNullable(originalCastFunction);
    }

    throw new RuntimeException(
       String.format("replacing cast function for datatype %s is not defined", dataMode));
  }

  /**
  * Check if a replacing cast function is available for the the original function
  * @param originalfunction original cast function
  * @return
  */
  public static boolean isReplacementNeeded(MinorType inputType, String originalfunction) {
    return inputType == MinorType.VARCHAR && CAST_FUNC_REPLACEMENT_NEEDED.contains(originalfunction);
  }

  private static String getReplacingCastFunctionFromNonNullable(String originalCastFunction) {
    if(CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE.containsKey(originalCastFunction)) {
      return CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE.get(originalCastFunction);
    }

    throw new RuntimeException(
      String.format("replacing cast function for %s is not defined", originalCastFunction));
  }

  private static String getReplacingCastFunctionFromNullable(String originalCastFunction) {
    if(CAST_FUNC_REPLACEMENT_FROM_NULLABLE.containsKey(originalCastFunction)) {
      return CAST_FUNC_REPLACEMENT_FROM_NULLABLE.get(originalCastFunction);
    }

    throw new RuntimeException(
      String.format("replacing cast function for %s is not defined", originalCastFunction));
  }
}
