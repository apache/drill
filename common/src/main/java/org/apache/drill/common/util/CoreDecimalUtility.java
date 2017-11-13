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
package org.apache.drill.common.util;

import java.math.BigDecimal;

import org.apache.drill.common.types.TypeProtos;

public class CoreDecimalUtility {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CoreDecimalUtility.class);

  public static long getDecimal18FromBigDecimal(BigDecimal input, int scale, int precision) {
    // Truncate or pad to set the input to the correct scale
    input = input.setScale(scale, BigDecimal.ROUND_HALF_UP);

    return (input.unscaledValue().longValue());
  }

  public static int getMaxPrecision(TypeProtos.MinorType decimalType) {
    if (decimalType == TypeProtos.MinorType.DECIMAL9) {
      return 9;
    } else if (decimalType == TypeProtos.MinorType.DECIMAL18) {
      return 18;
    } else if (decimalType == TypeProtos.MinorType.DECIMAL28SPARSE) {
      return 28;
    } else if (decimalType == TypeProtos.MinorType.DECIMAL38SPARSE) {
      return 38;
    }
    return 0;
  }

  /*
   * Function returns the Minor decimal type given the precision
   */
  public static TypeProtos.MinorType getDecimalDataType(int precision) {
    if (precision <= 9) {
      return TypeProtos.MinorType.DECIMAL9;
    } else if (precision <= 18) {
      return TypeProtos.MinorType.DECIMAL18;
    } else if (precision <= 28) {
      return TypeProtos.MinorType.DECIMAL28SPARSE;
    } else {
      return TypeProtos.MinorType.DECIMAL38SPARSE;
    }
  }

  /*
   * Given a precision it provides the max precision of that decimal data type;
   * For eg: given the precision 12, we would use DECIMAL18 to store the data
   * which has a max precision range of 18 digits
   */
  public static int getPrecisionRange(int precision) {
    return getMaxPrecision(getDecimalDataType(precision));
  }
  public static int getDecimal9FromBigDecimal(BigDecimal input, int scale, int precision) {
    // Truncate/ or pad to set the input to the correct scale
    input = input.setScale(scale, BigDecimal.ROUND_HALF_UP);

    return (input.unscaledValue().intValue());
  }

  /*
   * Helper function to detect if the given data type is Decimal
   */
  public static boolean isDecimalType(TypeProtos.MajorType type) {
    return isDecimalType(type.getMinorType());
  }

  public static boolean isDecimalType(TypeProtos.MinorType minorType) {
    return minorType == TypeProtos.MinorType.DECIMAL9 ||
           minorType == TypeProtos.MinorType.DECIMAL18 ||
           minorType == TypeProtos.MinorType.DECIMAL28SPARSE ||
           minorType == TypeProtos.MinorType.DECIMAL38SPARSE;
  }
}
