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

/*
 * Here we compute the scale and precision of the output decimal data type
 * based on the input scale and precision. Since division operation can be
 * a multiplication operation we compute the scale to be the sum of the inputs.
 * The precision is computed by getting the sum of integer digits of the input
 * and adding it with scale. The scale is further expanded to occupy the remaining
 * digits in the given precision range
 *
 * Eg: Input1 : precision = 5, scale = 3 ==> max integer digits = 2
 *     Input2 : precision = 7, scale = 4 ==> max integer digits = 3
 *
 *     Output: max integer digits ==> 2 + 3 = 5
 *             max scale          ==> 3 + 4 = 7
 *
 *             Minimum precision required ==> 5 + 7 = 12
 *
 * Since our minimum precision required is 12, we will use DECIMAL18 as the output type
 * but since this is divide we will grant the remaining digits in DECIMAL18 to scale
 * so we have the following
 *    output scale      ==> 7 + (18 - 12) = 13
 *    output precision  ==> 18
 */
public class DecimalScalePrecisionDivideFunction {
  private int outputScale = 0;
  private int outputPrecision = 0;

  public DecimalScalePrecisionDivideFunction(int leftPrecision, int leftScale, int rightPrecision, int rightScale) {
    // compute the output scale and precision here
    outputScale = leftScale + rightScale;
    int integerDigits = (leftPrecision - leftScale) + (rightPrecision - rightScale);

    outputPrecision = DecimalUtility.getPrecisionRange(outputScale + integerDigits);

    // Try and increase the scale if we have any room
    outputScale = (outputPrecision - integerDigits >= 0) ? (outputPrecision - integerDigits) : 0;
  }

  public int getOutputScale() {
    return outputScale;
  }

  public int getOutputPrecision() {
    return outputPrecision;
  }
}
