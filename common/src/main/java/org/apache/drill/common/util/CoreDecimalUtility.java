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
    return minorType == TypeProtos.MinorType.VARDECIMAL ||
           minorType == TypeProtos.MinorType.DECIMAL9 ||
           minorType == TypeProtos.MinorType.DECIMAL18 ||
           minorType == TypeProtos.MinorType.DECIMAL28SPARSE ||
           minorType == TypeProtos.MinorType.DECIMAL38SPARSE;
  }
}
